package org.apache.blur.titan;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;
import com.spatial4j.core.shape.Shape;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.indexing.IndexMutation;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAnd;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyAtom;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyCondition;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyNot;
import com.thinkaurelius.titan.graphdb.query.keycondition.KeyOr;
import com.thinkaurelius.titan.graphdb.query.keycondition.Relation;

public class BlurIndex implements IndexProvider {

  private static Log LOG = LogFactory.getLog(BlurIndex.class);

  private static final String TITAN = "titan";
  private static final String BLUR_FAMILY_DEFAULT = "blur.family.default";
  private static final String BLUR_TABLE_DEFAULT_SHARD_COUNT = "blur.table.default.shard.count";
  private static final String BLUR_CONTROLLER_CONNECTION = "blur.controller.connection";
  private static final String BLUR_TABLE_PREFIX = "blur.table.prefix";
  private static final String BLUR_WRITE_AHEAD_LOG = "blur.write.ahead.log";
  private static final String BLUR_WAIT_TO_BE_VISIBLE = "blur.wait.to.be.visible";

  private static final String DEFAULT_TABLE_NAME_PREFIX = "titan_";
  private static final int DEFAULT_SHARD_COUNT = 3;
  private static final boolean DEFAULT_BLUR_WAIT_TO_BE_VISIBLE = false;
  private static final boolean DEFAULT_BLUR_WRITE_AHEAD_LOG = true;

  private final String _tableNamePrefix;
  private final String _controllerConnectionString;
  private final int _defaultShardCount;
  private final String _family;
  private final boolean _waitToBeVisible;
  private final boolean _wal;

  public BlurIndex(Configuration config) {
    _tableNamePrefix = config.getString(BLUR_TABLE_PREFIX, DEFAULT_TABLE_NAME_PREFIX);
    _defaultShardCount = config.getInt(BLUR_TABLE_DEFAULT_SHARD_COUNT, DEFAULT_SHARD_COUNT);
    _controllerConnectionString = config.getString(BLUR_CONTROLLER_CONNECTION, "");
    _family = config.getString(BLUR_FAMILY_DEFAULT, TITAN);
    _waitToBeVisible = config.getBoolean(BLUR_WAIT_TO_BE_VISIBLE, DEFAULT_BLUR_WAIT_TO_BE_VISIBLE);
    _wal = config.getBoolean(BLUR_WRITE_AHEAD_LOG, DEFAULT_BLUR_WRITE_AHEAD_LOG);
    Preconditions.checkArgument(StringUtils.isNotBlank(_controllerConnectionString),
        "Need to configure connection string for Blur (" + BLUR_CONTROLLER_CONNECTION + ")");
    LOG.info("Blur using connection [" + _controllerConnectionString + "] with table prefix of [" + _tableNamePrefix
        + "]");
  }

  @Override
  public void mutate(Map<String, Map<String, IndexMutation>> mutations, TransactionHandle tx) throws StorageException {
    Iface client = getClient();
    List<RowMutation> mutationBatch = new ArrayList<RowMutation>();
    for (Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
      String store = stores.getKey();
      String tableName = getTableName(store);
      for (Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
        String rowId = entry.getKey();
        IndexMutation mutation = entry.getValue();

        RowMutation rowMutation = new RowMutation();
        rowMutation.setRowId(rowId);
        rowMutation.setTable(tableName);
        rowMutation.setWal(_wal);
        rowMutation.setWaitToBeVisible(_waitToBeVisible);
        mutationBatch.add(rowMutation);

        if (mutation.isDeleted()) {
          rowMutation.setRowMutationType(RowMutationType.DELETE_ROW);
          continue;
        }

        RecordMutation recordMutation = new RecordMutation().setRecordMutationType(RecordMutationType.REPLACE_COLUMNS);
        Record record = new Record().setFamily(getFamily(store)).setRecordId(rowId);

        rowMutation.addToRecordMutations(recordMutation);
        rowMutation.setRowMutationType(RowMutationType.UPDATE_ROW);

        if (mutation.hasAdditions()) {
          for (IndexEntry indexEntry : mutation.getAdditions()) {
            record.addToColumns(new Column(indexEntry.key, getValue(indexEntry.value)));
          }
        }
        if (mutation.hasDeletions()) {
          for (IndexEntry indexEntry : mutation.getAdditions()) {
            record.addToColumns(new Column(indexEntry.key, null));
          }
        }
      }
    }
    try {
      client.mutateBatch(mutationBatch);
    } catch (BlurException e) {
      throw new PermanentStorageException("Unknown error while trying to perform batch update.", e);
    } catch (TException e) {
      throw new PermanentStorageException("Unknown error while trying to perform batch update.", e);
    }
  }

  @Override
  public List<String> query(IndexQuery indexQuery, TransactionHandle tx) throws StorageException {
    KeyCondition<String> condition = indexQuery.getCondition();
    String store = indexQuery.getStore();
    String family = getFamily(store);
    String queryString = getQueryString(family, condition);
    Query query = new Query();
    query.setQuery(queryString);
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setQuery(query);
    if (indexQuery.hasLimit()) {
      blurQuery.setFetch(indexQuery.getLimit());
    }

    String tableName = getTableName(store);
    Iface client = getClient();
    try {
      BlurResults results = client.query(tableName, blurQuery);
      List<String> rowIds = new ArrayList<String>();
      for (BlurResult result : results.getResults()) {
        FetchResult fetchResult = result.getFetchResult();
        FetchRowResult rowResult = fetchResult.getRowResult();
        String id = rowResult.getRow().getId();
        rowIds.add(id);
      }
      return rowIds;
    } catch (BlurException e) {
      throw new PermanentStorageException("Unknown error while trying to query store [" + store + "] with indexquery ["
          + indexQuery + "].", e);
    } catch (TException e) {
      throw new PermanentStorageException("Unknown error while trying to query store [" + store + "] with indexquery ["
          + indexQuery + "].", e);
    }
  }

  private String getQueryString(String family, KeyCondition<String> condition) {
    if (condition instanceof KeyAtom) {
      KeyAtom<String> atom = (KeyAtom<String>) condition;
      Object value = atom.getCondition();
      String key = atom.getKey();
      Relation relation = atom.getRelation();
      if (value instanceof Number || value instanceof Interval) {
        Preconditions.checkArgument(relation instanceof Cmp, "Relation not supported on numeric types: " + relation);
        if (relation == Cmp.INTERVAL) {
          Preconditions.checkArgument(value instanceof Interval && ((Interval<?>) value).getStart() instanceof Number);
          Interval<?> i = (Interval<?>) value;
          StringBuilder builder = new StringBuilder();
          String columnName = getColumnName(family, key);
          builder.append(columnName).append(':');
          if (i.startInclusive()) {
            builder.append('[');
          } else {
            builder.append('{');
          }
          builder.append(i.getStart()).append(" TO ").append(i.getEnd());
          if (i.endInclusive()) {
            builder.append(']');
          } else {
            builder.append('}');
          }
          return builder.toString();
        } else {
          Preconditions.checkArgument(value instanceof Number);
          return getQueryString(family, key, (Cmp) relation, (Number) value);
        }
      } else if (value instanceof String) {
        if (relation == Text.CONTAINS) {
          return (String) value;
        } else
          throw new IllegalArgumentException("Relation is not supported for string value: " + relation);
      } else if (value instanceof Geoshape) {
        Preconditions.checkArgument(relation == Geo.INTERSECT, "Relation is not supported for geo value: " + relation);
        Shape shape = ((Geoshape) value).convert2Spatial4j();
        return "Intersects(" + shape.toString() + ")";
      } else {
        throw new IllegalArgumentException("Unsupported type: " + value);
      }
    } else if (condition instanceof KeyNot) {
      return "-(" + getQueryString(family, ((KeyNot<String>) condition).getChild()) + ")";
    } else if (condition instanceof KeyAnd) {
      StringBuilder builder = new StringBuilder("(");
      for (KeyCondition<String> c : condition.getChildren()) {
        builder.append("+").append(getQueryString(family, c)).append(' ');
      }
      return builder.toString();
    } else if (condition instanceof KeyOr) {
      StringBuilder builder = new StringBuilder("(");
      for (KeyCondition<String> c : condition.getChildren()) {
        builder.append(getQueryString(family, c)).append(' ');
      }
      return builder.toString();
    } else {
      throw new IllegalArgumentException("Invalid condition: " + condition);
    }
  }

  private String getColumnName(String family, String key) {
    return family + "." + key;
  }

  private final String getQueryString(String family, String key, Cmp relation, Number value) {
    String columnName = getColumnName(family, key);
    switch (relation) {
    case EQUAL:
      return columnName + ":" + value;
    case NOT_EQUAL:
      return "-(" + columnName + ":" + value + ")";
    case LESS_THAN:
      return columnName + ":[MIN TO " + value + "}";
    case LESS_THAN_EQUAL:
      return columnName + ":[MIN TO " + value + "]";
    case GREATER_THAN:
      return columnName + ":{" + value + " TO MAX]";
    case GREATER_THAN_EQUAL:
      return columnName + ":[" + value + " TO MAX]";
    default:
      throw new IllegalArgumentException("Unexpected relation: " + relation);
    }
  }

  @Override
  public boolean supports(Class<?> dataType, Relation relation) {
    if (Number.class.isAssignableFrom(dataType)) {
      if (relation instanceof Cmp) {
        return true;
      }
    } else if (dataType == Geoshape.class) {
      return relation == Geo.INTERSECT;
    } else if (dataType == String.class) {
      return relation == Text.CONTAINS;
    }
    return false;
  }

  @Override
  public boolean supports(Class<?> dataType) {
    if (Number.class.isAssignableFrom(dataType) || dataType == Geoshape.class || dataType == String.class) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public TransactionHandle beginTransaction() throws StorageException {
    return TransactionHandle.NO_TRANSACTION;
  }

  @Override
  public void clearStorage() throws StorageException {
    LOG.info("Clearing storage");
    Iface client = getClient();
    try {
      List<String> tableList = client.tableList();
      for (String table : tableList) {
        if (table.startsWith(_tableNamePrefix)) {
          LOG.info("Clearing store table [" + table + "]");
          TableDescriptor describe = client.describe(table);
          LOG.info("Disabling table [" + table + "]");
          client.disableTable(table);
          LOG.info("Removing table [" + table + "]");
          client.removeTable(table, true);
          LOG.info("Creating table [" + table + "]");
          client.createTable(describe);
        }
      }
    } catch (BlurException e) {
      throw new PermanentStorageException("Unknown error while trying to clear storage.", e);
    } catch (TException e) {
      throw new PermanentStorageException("Unknown error while trying to clear storage.", e);
    }
  }

  @Override
  public void close() throws StorageException {
    // Do Nothing
  }

  @Override
  public void register(String store, String key, Class<?> dataType, TransactionHandle tx) throws StorageException {
    LOG.info("Registering key [" + key + "] with dataType [" + dataType + "] in store [" + store + "]");
    String tableName = getTableName(store);
    Iface client = getClient();
    String family = getFamily(store);
    try {
      createTableIfMissing(tableName, client);
      if (dataType.equals(Integer.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "int", null));
      } else if (dataType.equals(Long.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "long", null));
      } else if (dataType.equals(Double.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "double", null));
      } else if (dataType.equals(Float.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "float", null));
      } else if (dataType.equals(Geoshape.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "geo-pointvector", null));
      } else if (dataType.equals(String.class)) {
        client.addColumnDefinition(tableName, new ColumnDefinition(family, key, null, false, "text", null));
      } else {
        throw new IllegalArgumentException("Unsupported type: " + dataType);
      }
    } catch (BlurException e) {
      LOG.error("Unknown error while trying to registered new type. Store [" + store + "] Key [" + key + "] dateType ["
          + dataType + "]");
      throw new PermanentStorageException("Unknown error while trying to registered new type. Store [" + store
          + "] Key [" + key + "] dateType [" + dataType + "]", e);
    } catch (TException e) {
      LOG.error("Unknown error while trying to registered new type. Store [" + store + "] Key [" + key + "] dateType ["
          + dataType + "]");
      throw new PermanentStorageException("Unknown error while trying to registered new type. Store [" + store
          + "] Key [" + key + "] dateType [" + dataType + "]", e);
    }
  }

  private void createTableIfMissing(String tableName, Iface client) throws BlurException, TException {
    List<String> tableList = client.tableList();
    if (tableList.contains(tableName)) {
      return;
    }
    LOG.info("Table [" + tableName + "] missing, creating with default shard count [" + _defaultShardCount + "]");
    TableDescriptor td = new TableDescriptor();
    td.setName(tableName);
    td.setShardCount(_defaultShardCount);
    client.createTable(td);
  }

  private String getFamily(String store) {
    return _family;
  }

  private String getTableName(String store) {
    return _tableNamePrefix + store;
  }

  private Iface getClient() {
    return BlurClient.getClient(_controllerConnectionString);
  }

  private String getValue(Object value) {
    if (value instanceof Number) {
      return value.toString();
    } else if (value instanceof String) {
      return (String) value;
    } else if (value instanceof Geoshape) {
      Shape shape = ((Geoshape) value).convert2Spatial4j();
      return shape.toString();
    }
    throw new IllegalArgumentException("Unsupported type: " + value);
  }

}
