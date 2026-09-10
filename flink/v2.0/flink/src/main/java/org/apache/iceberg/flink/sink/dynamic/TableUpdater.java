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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Updates the Iceberg tables in case of schema, branch, or partition changes. */
@Internal
class TableUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(TableUpdater.class);

  /**
   * Retry budget for a metadata commit which lost the pointer race. Kept small and bounded: this
   * runs on the record path, so the subtask is blocked while it backs off.
   */
  private static final int COMMIT_CONFLICT_RETRIES = 3;

  private static final long MIN_RETRY_WAIT_MS = 100;
  private static final long MAX_RETRY_WAIT_MS = 2_000;
  private static final long TOTAL_RETRY_TIME_MS = 10_000;
  private static final double RETRY_SCALE_FACTOR = 2.0;

  private final TableMetadataCache cache;
  private final Catalog catalog;
  private final boolean caseSensitive;
  private final boolean dropUnusedColumns;

  TableUpdater(
      TableMetadataCache cache, Catalog catalog, boolean caseSensitive, boolean dropUnusedColumns) {
    this.cache = cache;
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
    this.dropUnusedColumns = dropUnusedColumns;
  }

  /**
   * Creates or updates a table to make sure that the given branch, schema, spec exists.
   *
   * @return a {@link Tuple3} of the new {@link Schema}, the status of the schema compared to the
   *     requested one, and the new {@link PartitionSpec#specId()}.
   */
  Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> update(
      TableIdentifier tableIdentifier,
      String branch,
      Schema schema,
      PartitionSpec spec,
      TableCreator tableCreator) {
    findOrCreateTable(tableIdentifier, schema, spec, tableCreator);
    findOrCreateBranch(tableIdentifier, branch);
    TableMetadataCache.ResolvedSchemaInfo newSchemaInfo =
        findOrCreateSchema(tableIdentifier, schema);
    PartitionSpec newSpec = findOrCreateSpec(tableIdentifier, spec);
    return Tuple2.of(newSchemaInfo, newSpec);
  }

  private void findOrCreateTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, TableCreator tableCreator) {
    Tuple2<Boolean, Exception> exists = cache.exists(identifier);
    if (Boolean.FALSE.equals(exists.f0)) {
      if (exists.f1 instanceof NoSuchNamespaceException) {
        SupportsNamespaces catalogWithNameSpace = (SupportsNamespaces) catalog;
        LOG.info("Namespace {} not found during table search. Creating namespace", identifier);
        try {
          catalogWithNameSpace.createNamespace(identifier.namespace());
        } catch (AlreadyExistsException e) {
          LOG.debug("Namespace {} created concurrently", identifier.namespace(), e);
        }
      }

      LOG.info("Table {} not found during table search. Creating table.", identifier);
      try {
        Table table = tableCreator.createTable(catalog, identifier, schema, spec);
        cache.update(identifier, table);
      } catch (AlreadyExistsException e) {
        LOG.debug("Table {} created concurrently. Skipping creation.", identifier, e);
        cache.invalidate(identifier);
        findOrCreateTable(identifier, schema, spec, tableCreator);
      }
    }
  }

  private void findOrCreateBranch(TableIdentifier identifier, String branch) {
    String fromCache = cache.branch(identifier, branch);
    if (fromCache == null) {
      Table table = catalog.loadTable(identifier);
      try {
        table.manageSnapshots().createBranch(branch).commit();
        LOG.info("Branch {} for {} created", branch, identifier);
      } catch (CommitFailedException e) {
        table.refresh();
        if (table.refs().containsKey(branch)) {
          LOG.debug("Branch {} concurrently created for {}.", branch, identifier);
        } else {
          LOG.error("Failed to create branch {} for {}.", branch, identifier, e);
          throw e;
        }
      }

      cache.update(identifier, table);
    }
  }

  private TableMetadataCache.ResolvedSchemaInfo findOrCreateSchema(
      TableIdentifier identifier, Schema schema) {
    TableMetadataCache.ResolvedSchemaInfo fromCache = cache.schema(identifier, schema);
    if (fromCache.compareResult() != CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED) {
      return fromCache;
    }

    return retryOnCommitConflict(identifier, id -> evolveSchema(id, schema));
  }

  private TableMetadataCache.ResolvedSchemaInfo evolveSchema(
      TableIdentifier identifier, Schema schema) {
    Table table = catalog.loadTable(identifier);
    Schema tableSchema = table.schema();
    CompareSchemasVisitor.Result result =
        CompareSchemasVisitor.visit(schema, tableSchema, caseSensitive, dropUnusedColumns);
    switch (result) {
      case SAME:
        cache.update(identifier, table);
        return new TableMetadataCache.ResolvedSchemaInfo(
            tableSchema, result, DataConverter.identity());
      case DATA_CONVERSION_NEEDED:
        cache.update(identifier, table);
        return new TableMetadataCache.ResolvedSchemaInfo(
            tableSchema,
            result,
            DataConverter.get(
                FlinkSchemaUtil.convert(schema), FlinkSchemaUtil.convert(tableSchema)));
      case SCHEMA_UPDATE_NEEDED:
        LOG.info("Triggering schema update for table {} {} to {}", identifier, tableSchema, schema);
        UpdateSchema updateApi = table.updateSchema();
        EvolveSchemaVisitor.visit(
            identifier, updateApi, tableSchema, schema, caseSensitive, dropUnusedColumns);

        try {
          updateApi.commit();
          cache.update(identifier, table);
          TableMetadataCache.ResolvedSchemaInfo comparisonAfterMigration =
              cache.schema(identifier, schema);
          Schema newSchema = comparisonAfterMigration.resolvedTableSchema();
          LOG.info("Table {} schema updated from {} to {}", identifier, tableSchema, newSchema);
          return comparisonAfterMigration;
        } catch (CommitFailedException e) {
          cache.invalidate(identifier);
          TableMetadataCache.ResolvedSchemaInfo newSchema = cache.schema(identifier, schema);
          if (newSchema.compareResult() != CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED) {
            LOG.debug("Table {} schema updated concurrently to {}", identifier, schema);
            return newSchema;
          } else {
            LOG.warn(
                "Schema update for {} from {} to {} lost the commit race",
                identifier,
                tableSchema,
                schema,
                e);
            throw e;
          }
        }
      default:
        throw new IllegalArgumentException("Unknown comparison result");
    }
  }

  private PartitionSpec findOrCreateSpec(TableIdentifier identifier, PartitionSpec targetSpec) {
    PartitionSpec currentSpec = cache.spec(identifier, targetSpec);
    if (currentSpec != null) {
      return currentSpec;
    }

    return retryOnCommitConflict(identifier, id -> evolveSpec(id, targetSpec));
  }

  private PartitionSpec evolveSpec(TableIdentifier identifier, PartitionSpec targetSpec) {
    Table table = catalog.loadTable(identifier);
    PartitionSpec currentSpec = table.spec();

    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(currentSpec, targetSpec);
    if (result.isEmpty()) {
      LOG.info("Returning equivalent existing spec {} for {}", currentSpec, targetSpec);
      return currentSpec;
    }

    LOG.info(
        "Spec for table {} has been altered. Updating from {} to {}",
        identifier,
        currentSpec,
        targetSpec);
    UpdatePartitionSpec updater = table.updateSpec();
    result.termsToRemove().forEach(updater::removeField);
    result.termsToAdd().forEach(updater::addField);

    try {
      updater.commit();
      cache.update(identifier, table);
    } catch (CommitFailedException e) {
      cache.invalidate(identifier);
      PartitionSpec newSpec = cache.spec(identifier, targetSpec);
      result = PartitionSpecEvolution.evolve(targetSpec, newSpec);
      if (result.isEmpty()) {
        LOG.debug("Table {} partition spec updated concurrently to {}", identifier, newSpec);
        return newSpec;
      } else {
        LOG.warn(
            "Partition spec update for {} from {} to {} lost the commit race",
            identifier,
            currentSpec,
            targetSpec,
            e);
        throw e;
      }
    }
    return cache.spec(identifier, targetSpec);
  }

  /**
   * Runs one metadata-evolution attempt, repeating it against freshly loaded metadata while the
   * commit keeps losing the pointer race. A concurrent commit which already covers what this
   * attempt wanted returns from the attempt itself; a concurrent commit which does not — two
   * writers introducing disjoint columns, or a table-property commit landing between the load and
   * the commit — leaves the update still needed, and re-evolving on the new metadata is what makes
   * progress. The exception from the final attempt propagates.
   */
  private static <T> T retryOnCommitConflict(
      TableIdentifier identifier, Function<TableIdentifier, T> attempt) {
    AtomicReference<T> result = new AtomicReference<>();
    Tasks.foreach(identifier)
        .retry(COMMIT_CONFLICT_RETRIES)
        .onlyRetryOn(CommitFailedException.class)
        .exponentialBackoff(
            MIN_RETRY_WAIT_MS, MAX_RETRY_WAIT_MS, TOTAL_RETRY_TIME_MS, RETRY_SCALE_FACTOR)
        .run(id -> result.set(attempt.apply(id)));
    return result.get();
  }
}
