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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.jdbc.abstractimpl.AbstractBlurDatabaseMetaData;
import org.apache.blur.jdbc.util.EmptyResultSet;
import org.apache.blur.jdbc.util.SimpleStringResultSet;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;


public class BlurDatabaseMetaData extends AbstractBlurDatabaseMetaData {

  private static final String REF_GENERATION = "REF_GENERATION";
  private static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
  private static final String TYPE_NAME = "TYPE_NAME";
  private static final String TYPE_SCHEM = "TYPE_SCHEM";
  private static final String TYPE_CAT = "TYPE_CAT";
  private static final String REMARKS = "REMARKS";
  private static final String TABLE_TYPE = "TABLE_TYPE";
  private static final String TABLE_NAME = "TABLE_NAME";
  private static final String TABLE_SCHEM = "TABLE_SCHEM";
  private static final String TABLE_CAT = "TABLE_CAT";
  private static final String TABLE_CATALOG = "TABLE_CATALOG";

  private int minorVersion = 1;
  private int majorVersion = 1;
  private String url;
  private String username;
  private List<String> tables;
  private Map<String, Schema> schemaMap = new TreeMap<String, Schema>();

  public BlurDatabaseMetaData(String url, String username, String connectionString) {
    this.url = url;
    this.username = username;
    try {
      BlurClientManager.execute(connectionString, new BlurCommand<Void>() {
        @Override
        public Void call(Client client) throws BlurException, TException {
          tables = client.tableList();
          for (String table : tables) {
            TableDescriptor descriptor = client.describe(table);
            if (descriptor.isEnabled) {
              schemaMap.put(table, client.schema(table));
            }
          }
          return null;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    return super.getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    List<Map<String, String>> data = new ArrayList<Map<String, String>>();
    for (String table : tables) {
      Map<String, String> row = new HashMap<String, String>();
      row.put(TABLE_SCHEM, table);
      data.add(row);
    }
    return new SimpleStringResultSet(Arrays.asList(TABLE_SCHEM, TABLE_CATALOG), data);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    List<Map<String, String>> data = new ArrayList<Map<String, String>>();
    for (String table : tables) {
      if (tableNamePattern != null && !table.equals(tableNamePattern)) {
        continue;
      }
      Schema schema = schemaMap.get(table);
      Map<String, Set<String>> columnFamilies = schema.columnFamilies;
      addTableRow(data, table, table);
      for (String columnFamily : columnFamilies.keySet()) {
        String tablePlusCf = table + "." + columnFamily;
        if (tableNamePattern != null && !tablePlusCf.equals(tableNamePattern)) {
          continue;
        }
        addTableRow(data, table, tablePlusCf);
      }
    }
    return new TableDescriptorResultSet(data);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    return new EmptyResultSet();
  }

  private void addTableRow(List<Map<String, String>> data, String schem, String table) {
    Map<String, String> row = new HashMap<String, String>();
    row.put(TABLE_SCHEM, schem);
    row.put(TABLE_NAME, table);
    row.put(TABLE_TYPE, "TABLE");
    row.put(REMARKS, "");
    data.add(row);
  }

  public static class TableDescriptorResultSet extends SimpleStringResultSet {
    private static final List<String> COL_NAMES = Arrays.asList(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM, TYPE_NAME,
        SELF_REFERENCING_COL_NAME, REF_GENERATION);

    public TableDescriptorResultSet(List<Map<String, String>> data) {
      super(COL_NAMES, data);
    }
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return "";
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public ResultSet getTableTypes() throws SQLException {
    return new EmptyResultSet();
  }

  public String getDatabaseProductName() throws SQLException {
    return "blur";
  }

  public String getCatalogSeparator() throws SQLException {
    return ",";
  }

  public String getDriverName() throws SQLException {
    return "blur";
  }

  public String getDriverVersion() throws SQLException {
    return "0.1";
  }

  public String getIdentifierQuoteString() throws SQLException {
    return "'";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "0.1";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return majorVersion;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return minorVersion;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return majorVersion;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return minorVersion;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 1;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return "";
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "catalog";
  }

  @Override
  public int getDriverMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getDriverMinorVersion() {
    return minorVersion;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return ".";
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "procedure";
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public String getURL() throws SQLException {
    return url;
  }

  @Override
  public String getUserName() throws SQLException {
    return username;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    return new EmptyResultSet();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }



}
