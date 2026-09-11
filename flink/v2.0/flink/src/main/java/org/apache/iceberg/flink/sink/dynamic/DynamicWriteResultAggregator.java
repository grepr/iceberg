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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects to a single {@link
 * DynamicCommittable} per checkpoint (storing the serialized {@link DeltaManifests}, jobId,
 * operatorId, checkpointId)
 */
class DynamicWriteResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DynamicCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicWriteResultAggregator.class);

  private final CatalogLoader catalogLoader;
  private final int cacheMaximumSize;
  private transient Map<TableKey, Map<Integer, Collection<WriteResult>>> resultsByTableKeyAndSpec;
  private transient Map<String, Map<Integer, PartitionSpec>> specs;
  private transient Map<String, Tuple2<ManifestOutputFileFactory, Integer>>
      outputFileFactoriesAndFormatVersions;
  private transient String flinkJobId;
  private transient String operatorId;
  private transient int subTaskId;
  private transient int attemptId;
  private transient Catalog catalog;

  /** The last checkpoint this operator saw; Flink numbers them from 1, so 0 means none yet. */
  private long lastCheckpointId = 0L;

  /** Set once {@link #finish} has emitted; the final checkpoint must not emit a second summary. */
  private boolean endOfInput = false;

  DynamicWriteResultAggregator(CatalogLoader catalogLoader, int cacheMaximumSize) {
    this.catalogLoader = catalogLoader;
    this.cacheMaximumSize = cacheMaximumSize;
  }

  @Override
  public void open() throws Exception {
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorId = getOperatorID().toString();
    this.subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.resultsByTableKeyAndSpec = Maps.newHashMap();
    this.specs = new LRUCache<>(cacheMaximumSize);
    this.outputFileFactoriesAndFormatVersions = new LRUCache<>(cacheMaximumSize);
    this.catalog = catalogLoader.loadCatalog();
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      DynamicSinkUtil.closeCatalog(catalog);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    restoreLastCheckpointId(context.getRestoredCheckpointId());
  }

  /**
   * Seeds the checkpoint counter on restore, so {@link #finish()} emits above the last id already
   * committed rather than counting from zero.
   *
   * @param restoredCheckpointId the checkpoint this operator was restored from, if any
   */
  @VisibleForTesting
  void restoreLastCheckpointId(OptionalLong restoredCheckpointId) {
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
    Collection<CommittableWithLineage<DynamicCommittable>> committables =
        Sets.newHashSetWithExpectedSize(resultsByTableKeyAndSpec.size());
    int count = 0;
    for (Map.Entry<TableKey, Map<Integer, Collection<WriteResult>>> entries :
        resultsByTableKeyAndSpec.entrySet()) {
      committables.add(
          new CommittableWithLineage<>(
              new DynamicCommittable(
                  entries.getKey(),
                  writeToManifests(entries.getKey().tableName(), entries.getValue(), checkpointId),
                  getContainingTask().getEnvironment().getJobID().toString(),
                  getRuntimeContext().getOperatorUniqueID(),
                  checkpointId),
              checkpointId,
              count));
      ++count;
    }

    output.collect(
        new StreamRecord<>(
            new CommittableSummary<>(subTaskId, count, checkpointId, count, count, 0)));
    committables.forEach(
        c ->
            output.collect(
                new StreamRecord<>(
                    new CommittableWithLineage<>(c.getCommittable(), checkpointId, subTaskId))));
    LOG.info("Emitted {} commit message to downstream committer operator", count);
    resultsByTableKeyAndSpec.clear();
  }

  /**
   * Write all the completed data files to a newly created manifest files and return the manifests'
   * avro serialized bytes.
   */
  @VisibleForTesting
  byte[][] writeToManifests(
      String tableName, Map<Integer, Collection<WriteResult>> writeResultsBySpec, long checkpointId)
      throws IOException {
    byte[][] deltaManifestsBySpec = new byte[writeResultsBySpec.size()][];
    int idx = 0;
    for (Map.Entry<Integer, Collection<WriteResult>> entry : writeResultsBySpec.entrySet()) {
      deltaManifestsBySpec[idx] =
          writeToManifest(tableName, entry.getKey(), entry.getValue(), checkpointId);
      idx++;
    }

    return deltaManifestsBySpec;
  }

  private byte[] writeToManifest(
      String tableName, int specId, Collection<WriteResult> writeResults, long checkpointId)
      throws IOException {
    WriteResult.Builder builder = WriteResult.builder();
    writeResults.forEach(builder::add);
    WriteResult result = builder.build();

    Tuple2<ManifestOutputFileFactory, Integer> outputFileFactoryAndVersion =
        outputFileFactoryAndFormatVersion(tableName);
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> outputFileFactoryAndVersion.f0.create(checkpointId),
            spec(tableName, specId),
            outputFileFactoryAndVersion.f1);

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<DynamicWriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      DynamicWriteResult result =
          ((CommittableWithLineage<DynamicWriteResult>) element.getValue()).getCommittable();
      Collection<WriteResult> resultsPerTableKeyAndSpec =
          resultsByTableKeyAndSpec
              .computeIfAbsent(result.key(), unused -> Maps.newHashMap())
              .computeIfAbsent(result.specId(), unused -> Lists.newArrayList());
      resultsPerTableKeyAndSpec.add(result.writeResult());
      LOG.debug(
          "Added {}, specId={}, totalResults={}",
          result,
          result.specId(),
          resultsPerTableKeyAndSpec.size());
    }
  }

  private Tuple2<ManifestOutputFileFactory, Integer> outputFileFactoryAndFormatVersion(
      String tableName) {
    return outputFileFactoriesAndFormatVersions.computeIfAbsent(
        tableName,
        unused -> {
          Table table = catalog.loadTable(TableIdentifier.parse(tableName));
          specs.put(tableName, table.specs());
          // Make sure to append an identifier to avoid file clashes in case the factory was to get
          // re-created during a checkpoint, i.e. due to cache eviction.
          String fileSuffix = UUID.randomUUID().toString();
          ManifestOutputFileFactory outputFileFactory =
              FlinkManifestUtil.createOutputFileFactory(
                  () -> table,
                  table.properties(),
                  flinkJobId,
                  operatorId,
                  subTaskId,
                  attemptId,
                  fileSuffix);
          return Tuple2.of(outputFileFactory, TableUtil.formatVersion(table));
        });
  }

  private PartitionSpec spec(String tableName, int specId) {
    Map<Integer, PartitionSpec> knownSpecs = specs.get(tableName);
    if (knownSpecs != null) {
      PartitionSpec spec = knownSpecs.get(specId);
      if (spec != null) {
        return spec;
      }
    }

    Table table = catalog.loadTable(TableIdentifier.parse(tableName));
    return table.specs().get(specId);
  }
}
