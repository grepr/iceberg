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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Every operator of the dynamic sink loads a catalog of its own, and a catalog which is never
 * closed holds its client pool, HTTP connections, or metastore clients for the lifetime of the
 * TaskManager JVM — once per operator per restart. These tests count the catalogs handed out and
 * the ones closed, per operator and for a whole job, and require the two to be equal.
 */
class TestDynamicSinkCatalogLifecycle extends TestFlinkIcebergSinkBase {

  private static final String TABLE = "catalog_lifecycle";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DATABASE, TABLE);
  private static final Schema SCHEMA = SimpleDataUtil.SCHEMA;

  private String counterId;
  private CountingCatalogLoader countingLoader;

  @BeforeEach
  void before(TestInfo testInfo) {
    this.counterId = testInfo.getTestMethod().orElseThrow().getName();
    CountingCatalogLoader.reset(counterId);
    this.countingLoader = new CountingCatalogLoader(counterId, CATALOG_EXTENSION.catalogLoader());
    this.env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(1);
  }

  @Test
  void testRecordProcessorClosesItsCatalogAndTheGenerator() throws Exception {
    RowGenerator generator = new RowGenerator(-1);
    DynamicRecordProcessor<String> processor =
        new DynamicRecordProcessor<>(
            generator,
            countingLoader,
            TableCreator.DEFAULT,
            sinkConf(),
            Maps.newHashMap(),
            new Configuration());

    try (OneInputStreamOperatorTestHarness<String, DynamicRecordInternal> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(processor))) {
      harness.open();
      harness.processElement("1,a", 1L);

      assertThat(CountingCatalogLoader.opened(counterId)).isEqualTo(1);
      assertThat(CountingCatalogLoader.closed(counterId)).isZero();
    }

    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
    assertThat(RowGenerator.closedCount()).isEqualTo(1);
  }

  /**
   * The generator is handed the processor's catalog rather than loading one of its own, so a
   * generator which needs catalog access adds no client to the fleet's count.
   */
  @Test
  void testRecordProcessorSharesItsCatalogWithTheGenerator() throws Exception {
    RowGenerator generator = new RowGenerator(-1);
    DynamicRecordProcessor<String> processor =
        new DynamicRecordProcessor<>(
            generator,
            countingLoader,
            TableCreator.DEFAULT,
            sinkConf(),
            Maps.newHashMap(),
            new Configuration());

    try (OneInputStreamOperatorTestHarness<String, DynamicRecordInternal> harness =
        new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(processor))) {
      harness.open();
      assertThat(RowGenerator.openedWithCatalog()).isTrue();
      assertThat(CountingCatalogLoader.opened(counterId)).isEqualTo(1);
    }
  }

  @Test
  void testWriterClosesItsCatalog() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);

    Catalog catalog = countingLoader.loadCatalog();
    DynamicWriter writer =
        new DynamicWriter(
            catalog,
            Maps.newHashMap(),
            new Configuration(),
            10,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    writer.write(recordInternal(), null);
    assertThat(CountingCatalogLoader.closed(counterId)).isZero();

    writer.close();

    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  @Test
  void testCommitterClosesItsCatalog() throws Exception {
    Catalog catalog = countingLoader.loadCatalog();
    DynamicCommitter committer =
        new DynamicCommitter(
            catalog,
            Maps.newHashMap(),
            false,
            1,
            "sinkId",
            new DynamicCommitterMetrics(new UnregisteredMetricsGroup()));
    assertThat(CountingCatalogLoader.closed(counterId)).isZero();

    committer.close();

    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  @Test
  void testWriteResultAggregatorClosesItsCatalog() throws Exception {
    try (OneInputStreamOperatorTestHarness<?, ?> harness =
        new OneInputStreamOperatorTestHarness<>(
            new DynamicWriteResultAggregator(countingLoader, 10))) {
      harness.open();
      assertThat(CountingCatalogLoader.opened(counterId)).isEqualTo(1);
      assertThat(CountingCatalogLoader.closed(counterId)).isZero();
    }

    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  @Test
  void testTableUpdateOperatorClosesItsCatalog() throws Exception {
    DynamicTableUpdateOperator operator =
        new DynamicTableUpdateOperator(countingLoader, TableCreator.DEFAULT, sinkConf());
    operator.open(null);
    operator.map(recordInternal());

    assertThat(CountingCatalogLoader.opened(counterId)).isEqualTo(1);
    assertThat(CountingCatalogLoader.closed(counterId)).isZero();

    operator.close();

    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  /**
   * The serializer cache lives inside a {@code TypeSerializer}, which Flink never closes, so it
   * releases the catalog it loads for a cache miss before returning rather than holding one.
   */
  @Test
  void testSerializerCacheClosesTheCatalogItLoadsOnAMiss() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SCHEMA);
    TableSerializerCache cache = new TableSerializerCache(countingLoader, 10);

    cache.serializerWithSchemaAndSpec(
        TABLE_IDENTIFIER.toString(), table.schema().schemaId(), table.spec().specId());

    assertThat(CountingCatalogLoader.opened(counterId)).isPositive();
    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  @Test
  void testAllCatalogsClosedAfterJobCompletion() throws Exception {
    runJob(Lists.newArrayList("1,a", "2,b", "3,c"), -1);

    assertThat(CountingCatalogLoader.opened(counterId)).isPositive();
    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
    assertThat(SimpleDataUtil.tableRecords(CATALOG_EXTENSION.catalog().loadTable(TABLE_IDENTIFIER)))
        .hasSize(3);
  }

  @Test
  void testAllCatalogsClosedAfterJobFailure() {
    assertThatThrownBy(() -> runJob(Lists.newArrayList("1,a", "2,b", "3,c"), 2))
        .rootCause()
        .hasMessageContaining(RowGenerator.FAILURE_MESSAGE);

    assertThat(CountingCatalogLoader.opened(counterId)).isPositive();
    assertThat(CountingCatalogLoader.leaked(counterId)).isZero();
  }

  private void runJob(List<String> rows, int failOnRow) throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    env.configure(configuration);

    DataStream<String> input = env.fromData(rows, Types.STRING);
    DynamicIcebergSink.forInput(input)
        .generator(new RowGenerator(failOnRow))
        .catalogLoader(countingLoader)
        .writeParallelism(1)
        .immediateTableUpdate(true)
        .append();

    env.execute("catalog lifecycle");
  }

  private static FlinkDynamicSinkConf sinkConf() {
    return new FlinkDynamicSinkConf(
        ImmutableMap.of(FlinkDynamicSinkOptions.CASE_SENSITIVE.key(), "true"), new Configuration());
  }

  private static DynamicRecordInternal recordInternal() {
    return new DynamicRecordInternal(
        TABLE_IDENTIFIER.toString(),
        SnapshotRef.MAIN_BRANCH,
        SCHEMA,
        GenericRowData.of(1, StringData.fromString("a")),
        PartitionSpec.unpartitioned(),
        0,
        false,
        Collections.emptySet());
  }

  /**
   * Emits one {@link DynamicRecord} per {@code "<id>,<data>"} line, and records that it was opened
   * with a catalog and closed. The counters are static because Flink serializes the generator to
   * the task; each test that reads them runs the generator in this JVM.
   */
  private static class RowGenerator implements DynamicRecordGenerator<String> {

    static final String FAILURE_MESSAGE = "induced generator failure";

    private static volatile boolean openedWithCatalog = false;
    private static volatile int closed = 0;

    private final int failOnId;

    RowGenerator(int failOnId) {
      this.failOnId = failOnId;
      openedWithCatalog = false;
      closed = 0;
    }

    static boolean openedWithCatalog() {
      return openedWithCatalog;
    }

    static int closedCount() {
      return closed;
    }

    @Override
    public void open(OpenContext openContext, Catalog catalog) {
      openedWithCatalog = catalog != null;
    }

    @Override
    public void generate(String inputRecord, Collector<DynamicRecord> out) {
      String[] parts = inputRecord.split(",", 2);
      int id = Integer.parseInt(parts[0]);
      if (id == failOnId) {
        throw new IllegalStateException(FAILURE_MESSAGE);
      }

      out.collect(
          new DynamicRecord(
              TABLE_IDENTIFIER,
              SnapshotRef.MAIN_BRANCH,
              SCHEMA,
              GenericRowData.of(id, StringData.fromString(parts[1])),
              PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              1));
    }

    @Override
    public void close() {
      closed++;
    }
  }
}
