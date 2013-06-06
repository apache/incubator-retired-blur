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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.jdbc.abstractimpl.AbstractBlurResultSetMetaData;


public class BlurResultSetMetaData extends AbstractBlurResultSetMetaData {

  public static final String RECORDID = "recordid";
  public static final String FAMILY = "family";
  public static final String LOCATIONID = "locationid";
  public static final String SCORE = "score";
  public static final String ROWID = "rowid";

  public enum QUERY_COLUMN_TYPE {
    ALL_COLUMNS, ALL_COLUMNS_IN_CF, ONE_COLUMN_IN_CF, ONE_COLUMN_IN_SYSTEM
  }

  public static Collection<String> systemCols;
  private List<String> sqlColumnNames;
  private String tableName;
  private Map<String, Set<String>> columnFamilies;

  static {
    systemCols = new HashSet<String>();
    systemCols.add(ROWID);
    systemCols.add(SCORE);
    systemCols.add(LOCATIONID);
    systemCols.add(FAMILY);
    systemCols.add(RECORDID);
  }

  public BlurResultSetMetaData(List<String> queryColumnNames, Map<String, Set<String>> columnFamilies) {
    this.columnFamilies = columnFamilies;
    this.sqlColumnNames = new ArrayList<String>();
    this.sqlColumnNames.add(null);
    for (String queryColumnName : queryColumnNames) {
      addReturnColumns(queryColumnName);
    }
  }

  private void addReturnColumns(String queryColumnName) {
    String col = queryColumnName.trim();
    if ("*".equals(col)) {
      addAllColumns();
      return;
    }
    int index = col.indexOf('.');
    if (index < 0) {
      addSystemColumn(col);
      return;
    }
    String columnFamily = col.substring(0, index);
    String columnName = col.substring(index + 1);
    if ("*".equals(columnName)) {
      addColumnFamily(columnFamily);
      return;
    }
    addColumn(columnFamily, columnName);
  }

  private void addColumn(String columnFamily, String columnName) {
    sqlColumnNames.add(columnFamily + "." + columnName);
  }

  private void addColumnFamily(String columnFamily) {
    Set<String> columns = columnFamilies.get(columnFamily);
    if (columns == null) {
      throw new RuntimeException("Column family [" + columnFamily + "] not found.");
    }
    for (String columnName : columns) {
      addColumn(columnFamily, columnName);
    }
  }

  private void addSystemColumn(String col) {
    String lcCol = col.toLowerCase();
    if (systemCols.contains(lcCol)) {
      sqlColumnNames.add(lcCol);
      return;
    }
    throw new RuntimeException("System column [" + col + "] not found.");
  }

  private void addAllColumns() {
    sqlColumnNames.add(ROWID);
    sqlColumnNames.add(FAMILY);
    sqlColumnNames.add(RECORDID);
    Map<String, Set<String>> columnFamilies = new TreeMap<String, Set<String>>(this.columnFamilies);
    for (String family : columnFamilies.keySet()) {
      Set<String> columnNames = columnFamilies.get(family);
      for (String columnName : columnNames) {
        sqlColumnNames.add(family + "." + columnName);
      }
    }
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return "java.lang.String";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return Types.VARCHAR;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return sqlColumnNames.size() - 1;
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return sqlColumnNames.get(column);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return "string";
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return 4000;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return 0;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return tableName;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return 100;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }
}
