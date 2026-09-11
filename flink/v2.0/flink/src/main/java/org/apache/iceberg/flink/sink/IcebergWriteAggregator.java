/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.OptionalLong;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects) to a single {@link
 * IcebergCommittable} per checkpoint (storing the serialized {@link
 * org.apache.iceberg.flink.sink.DeltaManifests}, jobId, operatorId, checkpointId)
 */
class IcebergWriteAggregator extends AbstractStreamOperator<CommittableMessage<IcebergCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteAggregator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  private final Collection<WriteResult> results;
  private transient ManifestOutputFileFactory icebergManifestOutputFileFactory;
  private transient Table table;
  private final TableLoader tableLoader;

  /** The last checkpoint this operator saw; Flink numbers them from 1, so 0 means none yet. */
  private long lastCheckpointId = 0L;

  /** Set once {@link #finish} has emitted; the final checkpoint must not emit a second summary. */
  private boolean endOfInput = false;

  IcebergWriteAggregator(TableLoader tableLoader) {
    this.results = Sets.newHashSet();
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() throws Exception {
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    String operatorId = getOperatorID().toString();
    int subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    Preconditions.checkArgument(
        subTaskId == 0, "The subTaskId must be zero in the IcebergWriteAggregator");
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.table = tableLoader.loadTable();

    this.icebergManifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, subTaskId, attemptId);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    OptionalLong restoredCheckpointId = context.getRestoredCheckpointId();
    if (restoredCheckpointId.isPresent()) {
      this.lastCheckpointId = restoredCheckpointId.getAsLong();
    }
  }

  /**
   * Emits the remaining committables at the id the final checkpoint will carry, which is how
   * Flink's own {@code SinkWriterOperator} ends its input.
   *
   * <p>Emitting at {@code Long.MAX_VALUE} strands them. {@code CommitterOperator} commits at {@code
   * endInput()} only when checkpointing is disabled or the job runs in {@code BATCH} mode;
   * otherwise it commits, on each completed checkpoint, only the committables at or below that
   * checkpoint's id. A real checkpoint id is never {@code Long.MAX_VALUE}, so in checkpointed
   * {@code STREAMING} mode that tail is never committed and its data files are orphaned.
   *
   * <p>The id has to be greater than the last one committed as well as reachable by the final
   * checkpoint: the committer skips every request at or below the table's {@code
   * flink.max-committed-checkpoint-id}, so a restored operator that had not checkpointed again
   * would drop this tail as stale if it counted from zero. That is why {@link #initializeState}
   * seeds the counter.
   *
   * <p>The final checkpoint still runs {@link #prepareSnapshotPreBarrier} after this, at the same
   * id, so the {@code endOfInput} flag stops it emitting a second summary for that checkpoint —
   * which the committable collector rejects. Flink's {@code SinkWriterOperator} carries the same
   * flag for the same reason.
   */
  @Override
  public void finish() throws IOException {
    if (!endOfInput) {
      endOfInput = true;
      emitCommittables(lastCheckpointId + 1);
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    if (!endOfInput) {
      this.lastCheckpointId = checkpointId;
      emitCommittables(checkpointId);
    }
  }

  private void emitCommittables(long checkpointId) throws IOException {
    IcebergCommittable committable =
        new IcebergCommittable(
            writeToManifest(results, checkpointId),
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId);
    CommittableMessage<IcebergCommittable> summary =
        new CommittableSummary<>(0, 1, checkpointId, 1, 1, 0);
    output.collect(new StreamRecord<>(summary));
    CommittableMessage<IcebergCommittable> message =
        new CommittableWithLineage<>(committable, checkpointId, 0);
    output.collect(new StreamRecord<>(message));
    LOG.info("Emitted commit message to downstream committer operator");
    results.clear();
  }

  /**
   * Write all the completed data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  public byte[] writeToManifest(Collection<WriteResult> writeResults, long checkpointId)
      throws IOException {
    if (writeResults.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResults).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> icebergManifestOutputFileFactory.create(checkpointId),
            table.spec(),
            TableUtil.formatVersion(table));

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<WriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      results.add(((CommittableWithLineage<WriteResult>) element.getValue()).getCommittable());
    }
  }
}
