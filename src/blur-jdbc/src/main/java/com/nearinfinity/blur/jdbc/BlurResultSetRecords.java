package com.nearinfinity.blur.jdbc;

import static com.nearinfinity.blur.jdbc.BlurResultSetMetaData.FAMILY;
import static com.nearinfinity.blur.jdbc.BlurResultSetMetaData.LOCATIONID;
import static com.nearinfinity.blur.jdbc.BlurResultSetMetaData.RECORDID;
import static com.nearinfinity.blur.jdbc.BlurResultSetMetaData.ROWID;
import static com.nearinfinity.blur.jdbc.BlurResultSetMetaData.SCORE;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.thrift.TException;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurResultSet;
import com.nearinfinity.blur.jdbc.parser.Parser;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

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
    blurQuery.uuid = new Random().nextLong();
    blurQuery.fetch = fetch;
    blurQuery.start = start;
    blurQuery.simpleQuery = new SimpleQuery();
    blurQuery.simpleQuery.queryStr = parser.getWhere();
    blurQuery.simpleQuery.superQueryOn = false;

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
    selector.recordOnly = !blurQuery.simpleQuery.superQueryOn;
    Map<String, Set<String>> columnFamilies = schema.columnFamilies;
    Set<String> cfSet = columnFamilies.get(columnFamily);
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
          selector.setLocationId(result.getLocationId());
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
