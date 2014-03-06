package org.apache.blur.jdbc;

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
import static org.apache.blur.jdbc.BlurResultSetMetaData.FAMILY;
import static org.apache.blur.jdbc.BlurResultSetMetaData.LOCATIONID;
import static org.apache.blur.jdbc.BlurResultSetMetaData.RECORDID;
import static org.apache.blur.jdbc.BlurResultSetMetaData.ROWID;
import static org.apache.blur.jdbc.BlurResultSetMetaData.SCORE;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.blur.jdbc.abstractimpl.AbstractBlurResultSet;
import org.apache.blur.jdbc.parser.Parser;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.Selector;


public class BlurResultSetRecords extends AbstractBlurResultSet {

  private Selector selector;
  private BlurResults results;
  private int resultPosition = -1;
  private int size;
  private String tableName;
  private FetchResult fetchResult;
  private BlurResultSetMetaData blurResultSetMetaData;
  private String lastValue;
  private List<List<String>> displayRows = new ArrayList<List<String>>();
  private int displayRowsPosition;
  private Schema schema;
  private final Iface client;
  private long totalResults;
  private int overallRowPosition;
  private int currentFetch;
  private int currentStart;
  private Parser parser;
  private List<String> columnNames;
  private String columnFamily;

  public BlurResultSetRecords(Iface client, Parser parser) throws SQLException {
    this.client = client;
    this.parser = parser;
    String tName = parser.getTableName();
    int lastIndexOf = tName.lastIndexOf('.');
    tableName = tName.substring(0, lastIndexOf);
    columnFamily = tName.substring(lastIndexOf + 1);
    columnNames = parser.getColumnNames();
    runSearch(0, 100);
  }

  private void runSearch(int start, int fetch) throws SQLException {
    currentStart = start;
    currentFetch = fetch;
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.minimumNumberOfResults = Long.MAX_VALUE;
    blurQuery.maxQueryTime = Long.MAX_VALUE;
    blurQuery.uuid = UUID.randomUUID().toString();
    blurQuery.fetch = fetch;
    blurQuery.start = start;
    blurQuery.query = new Query();
    blurQuery.query.query = parser.getWhere();
    blurQuery.query.rowQuery = false;

    try {
      schema = client.schema(tableName);
    } catch (BlurException e) {
      e.printStackTrace();
      throw new SQLException(e);
    } catch (TException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }

    selector = new Selector();
    setupSelector(selector, schema, columnNames);
    selector.recordOnly = !blurQuery.query.rowQuery;
    Map<String, Map<String, ColumnDefinition>> columnFamilies = schema.getFamilies();
    Map<String, ColumnDefinition> cfSet = columnFamilies.get(columnFamily);
    columnFamilies.clear();
    columnFamilies.put(columnFamily, cfSet);
    blurResultSetMetaData = new BlurResultSetMetaData(columnNames, columnFamilies);
    try {
      results = client.query(tableName, blurQuery);
    } catch (BlurException e) {
      e.printStackTrace();
      throw new SQLException(e);
    } catch (TException e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
    if (results.totalResults > 0) {
      size = results.results.size();
    }
    totalResults = results.totalResults;
  }

  private void setupSelector(Selector selector, Schema schema, List<String> columnNames) {

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return blurResultSetMetaData;
  }

  @Override
  public boolean next() throws SQLException {
    if (displayRows != null && displayRowsPosition + 1 < displayRows.size()) {
      overallRowPosition++;
      displayRowsPosition++;
      return true;
    } else if (resultPosition + 1 < size) {
      INNER: while (resultPosition + 1 < size) {
        displayRows.clear();
        resultPosition++;
        displayRowsPosition = 0;
        overallRowPosition++;

        final BlurResult result = results.results.get(resultPosition);
        try {
          FetchRecordResult recordResult = result.getFetchResult().getRecordResult();
          selector.setRowId(recordResult.getRowid());
          selector.setRecordId(recordResult.getRecord().getRecordId());
          fetchResult = client.fetchRow(tableName, selector);
          Record record = fetchResult.recordResult.record;
          if (!record.family.equals(columnFamily)) {
            continue INNER;
          }
          String rowId = fetchResult.recordResult.rowid;
          displayRows.add(addColumns(result.getScore(), result.getLocationId(), rowId, record.family, record));
          return true;

        } catch (Exception e) {
          e.printStackTrace();
          throw new SQLException(e);
        }
      }
      return next();
    } else if (overallRowPosition < totalResults) {
      currentStart += currentFetch;
      runSearch(currentStart, currentFetch);
      displayRowsPosition = 0;
      resultPosition = -1;
      return next();
    }
    return false;
  }

  private List<String> addColumns(double score, String locationId, String rowId, String family, Record record) throws SQLException {
    int columnCount = blurResultSetMetaData.getColumnCount();
    List<String> result = new ArrayList<String>(columnCount + 1);
    for (int i = 0; i < columnCount + 1; i++) {
      result.add(null);
    }
    for (int i = 1; i <= columnCount; i++) {
      String columnName = blurResultSetMetaData.getColumnName(i);
      if (columnName.equals(ROWID)) {
        result.set(i, rowId);
      } else if (columnName.equals(SCORE)) {
        result.set(i, Double.toString(score));
      } else if (columnName.equals(LOCATIONID)) {
        result.set(i, locationId);
      } else if (columnName.equals(FAMILY)) {
        result.set(i, family);
      } else if (columnName.equals(RECORDID)) {
        result.set(i, record.recordId);
      } else {
        String value = getValue(record, columnName, family);
        result.set(i, value);
      }
    }
    return result;
  }

  private String getValue(Record record, String columnNameWithFamilyName, String family) {
    int index = columnNameWithFamilyName.indexOf('.');
    if (family.equals(columnNameWithFamilyName.substring(0, index))) {
      String columnName = columnNameWithFamilyName.substring(index + 1);
      List<String> values = new ArrayList<String>();
      for (Column col : record.columns) {
        if (columnName.equals(col.getName())) {
          values.add(col.getValue());
        }
      }
      if (values.size() == 1) {
        return values.get(0);
      } else {
        return values.toString();
      }
    } else {
      return null;
    }
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return lastValue = displayRows.get(displayRowsPosition).get(columnIndex);
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (lastValue == null) {
      return true;
    }
    return false;
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return new BlurWarnings();
  }
}
