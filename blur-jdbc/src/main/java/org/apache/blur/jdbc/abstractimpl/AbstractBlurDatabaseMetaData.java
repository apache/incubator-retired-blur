package org.apache.blur.jdbc.abstractimpl;

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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import org.apache.blur.jdbc.util.NotImplemented;

/**
 * This DatabaseMetaData implementation is simply to provide the major of the
 * method implementations that only throw not implemented exceptions. That way
 * it's easier to see what has been implemented in the real class.
 */
public abstract class AbstractBlurDatabaseMetaData implements DatabaseMetaData {

  private DatabaseMetaData throwExceptionDelegate;

  public AbstractBlurDatabaseMetaData() {
    throwExceptionDelegate = (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(),
        new Class[] { DatabaseMetaData.class }, new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Class<?> clazz = method.getReturnType();
            if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE)) {
              return false;
            }
            throw new NotImplemented(method.getName());
          }
        });
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return throwExceptionDelegate.allProceduresAreCallable();
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return throwExceptionDelegate.allTablesAreSelectable();
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return throwExceptionDelegate.autoCommitFailureClosesAllResultSets();
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return throwExceptionDelegate.dataDefinitionCausesTransactionCommit();
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return throwExceptionDelegate.dataDefinitionIgnoredInTransactions();
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    return throwExceptionDelegate.deletesAreDetected(type);
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return throwExceptionDelegate.doesMaxRowSizeIncludeBlobs();
  }

  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return throwExceptionDelegate.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return throwExceptionDelegate.getBestRowIdentifier(catalog, schema, table, scope, nullable);
  }

  public ResultSet getCatalogs() throws SQLException {
    return throwExceptionDelegate.getCatalogs();
  }

  public String getCatalogSeparator() throws SQLException {
    return throwExceptionDelegate.getCatalogSeparator();
  }

  public String getCatalogTerm() throws SQLException {
    return throwExceptionDelegate.getCatalogTerm();
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    return throwExceptionDelegate.getClientInfoProperties();
  }

  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return throwExceptionDelegate.getColumnPrivileges(catalog, schema, table, columnNamePattern);
  }

  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return throwExceptionDelegate.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  public Connection getConnection() throws SQLException {
    return throwExceptionDelegate.getConnection();
  }

  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return throwExceptionDelegate.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog,
        foreignSchema, foreignTable);
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return throwExceptionDelegate.getDatabaseMajorVersion();
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return throwExceptionDelegate.getDatabaseMinorVersion();
  }

  public String getDatabaseProductName() throws SQLException {
    return throwExceptionDelegate.getDatabaseProductName();
  }

  public String getDatabaseProductVersion() throws SQLException {
    return throwExceptionDelegate.getDatabaseProductVersion();
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return throwExceptionDelegate.getDefaultTransactionIsolation();
  }

  public int getDriverMajorVersion() {
    return throwExceptionDelegate.getDriverMajorVersion();
  }

  public int getDriverMinorVersion() {
    return throwExceptionDelegate.getDriverMinorVersion();
  }

  public String getDriverName() throws SQLException {
    return throwExceptionDelegate.getDriverName();
  }

  public String getDriverVersion() throws SQLException {
    return throwExceptionDelegate.getDriverVersion();
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return throwExceptionDelegate.getExportedKeys(catalog, schema, table);
  }

  public String getExtraNameCharacters() throws SQLException {
    return throwExceptionDelegate.getExtraNameCharacters();
  }

  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
      String columnNamePattern) throws SQLException {
    return throwExceptionDelegate.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return throwExceptionDelegate.getFunctions(catalog, schemaPattern, functionNamePattern);
  }

  public String getIdentifierQuoteString() throws SQLException {
    return throwExceptionDelegate.getIdentifierQuoteString();
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return throwExceptionDelegate.getImportedKeys(catalog, schema, table);
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return throwExceptionDelegate.getIndexInfo(catalog, schema, table, unique, approximate);
  }

  public int getJDBCMajorVersion() throws SQLException {
    return throwExceptionDelegate.getJDBCMajorVersion();
  }

  public int getJDBCMinorVersion() throws SQLException {
    return throwExceptionDelegate.getJDBCMinorVersion();
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return throwExceptionDelegate.getMaxBinaryLiteralLength();
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxCatalogNameLength();
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return throwExceptionDelegate.getMaxCharLiteralLength();
  }

  public int getMaxColumnNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxColumnNameLength();
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return throwExceptionDelegate.getMaxColumnsInGroupBy();
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return throwExceptionDelegate.getMaxColumnsInIndex();
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return throwExceptionDelegate.getMaxColumnsInOrderBy();
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return throwExceptionDelegate.getMaxColumnsInSelect();
  }

  public int getMaxColumnsInTable() throws SQLException {
    return throwExceptionDelegate.getMaxColumnsInTable();
  }

  public int getMaxConnections() throws SQLException {
    return throwExceptionDelegate.getMaxConnections();
  }

  public int getMaxCursorNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxCursorNameLength();
  }

  public int getMaxIndexLength() throws SQLException {
    return throwExceptionDelegate.getMaxIndexLength();
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxProcedureNameLength();
  }

  public int getMaxRowSize() throws SQLException {
    return throwExceptionDelegate.getMaxRowSize();
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxSchemaNameLength();
  }

  public int getMaxStatementLength() throws SQLException {
    return throwExceptionDelegate.getMaxStatementLength();
  }

  public int getMaxStatements() throws SQLException {
    return throwExceptionDelegate.getMaxStatements();
  }

  public int getMaxTableNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxTableNameLength();
  }

  public int getMaxTablesInSelect() throws SQLException {
    return throwExceptionDelegate.getMaxTablesInSelect();
  }

  public int getMaxUserNameLength() throws SQLException {
    return throwExceptionDelegate.getMaxUserNameLength();
  }

  public String getNumericFunctions() throws SQLException {
    return throwExceptionDelegate.getNumericFunctions();
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return throwExceptionDelegate.getPrimaryKeys(catalog, schema, table);
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    return throwExceptionDelegate.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
  }

  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    return throwExceptionDelegate.getProcedures(catalog, schemaPattern, procedureNamePattern);
  }

  public String getProcedureTerm() throws SQLException {
    return throwExceptionDelegate.getProcedureTerm();
  }

  public int getResultSetHoldability() throws SQLException {
    return throwExceptionDelegate.getResultSetHoldability();
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return throwExceptionDelegate.getRowIdLifetime();
  }

  public ResultSet getSchemas() throws SQLException {
    return throwExceptionDelegate.getSchemas();
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return throwExceptionDelegate.getSchemas(catalog, schemaPattern);
  }

  public String getSchemaTerm() throws SQLException {
    return throwExceptionDelegate.getSchemaTerm();
  }

  public String getSearchStringEscape() throws SQLException {
    return throwExceptionDelegate.getSearchStringEscape();
  }

  public String getSQLKeywords() throws SQLException {
    return throwExceptionDelegate.getSQLKeywords();
  }

  public int getSQLStateType() throws SQLException {
    return throwExceptionDelegate.getSQLStateType();
  }

  public String getStringFunctions() throws SQLException {
    return throwExceptionDelegate.getStringFunctions();
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return throwExceptionDelegate.getSuperTables(catalog, schemaPattern, tableNamePattern);
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    return throwExceptionDelegate.getSuperTypes(catalog, schemaPattern, typeNamePattern);
  }

  public String getSystemFunctions() throws SQLException {
    return throwExceptionDelegate.getSystemFunctions();
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return throwExceptionDelegate.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
  }

  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    return throwExceptionDelegate.getTables(catalog, schemaPattern, tableNamePattern, types);
  }

  public ResultSet getTableTypes() throws SQLException {
    return throwExceptionDelegate.getTableTypes();
  }

  public String getTimeDateFunctions() throws SQLException {
    return throwExceptionDelegate.getTimeDateFunctions();
  }

  public ResultSet getTypeInfo() throws SQLException {
    return throwExceptionDelegate.getTypeInfo();
  }

  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return throwExceptionDelegate.getUDTs(catalog, schemaPattern, typeNamePattern, types);
  }

  public String getURL() throws SQLException {
    return throwExceptionDelegate.getURL();
  }

  public String getUserName() throws SQLException {
    return throwExceptionDelegate.getUserName();
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return throwExceptionDelegate.getVersionColumns(catalog, schema, table);
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    return throwExceptionDelegate.insertsAreDetected(type);
  }

  public boolean isCatalogAtStart() throws SQLException {
    return throwExceptionDelegate.isCatalogAtStart();
  }

  public boolean isReadOnly() throws SQLException {
    return throwExceptionDelegate.isReadOnly();
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return throwExceptionDelegate.isWrapperFor(iface);
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    return throwExceptionDelegate.locatorsUpdateCopy();
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return throwExceptionDelegate.nullPlusNonNullIsNull();
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return throwExceptionDelegate.nullsAreSortedAtEnd();
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return throwExceptionDelegate.nullsAreSortedAtStart();
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return throwExceptionDelegate.nullsAreSortedHigh();
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return throwExceptionDelegate.nullsAreSortedLow();
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.othersDeletesAreVisible(type);
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.othersInsertsAreVisible(type);
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.othersUpdatesAreVisible(type);
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.ownDeletesAreVisible(type);
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.ownInsertsAreVisible(type);
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return throwExceptionDelegate.ownUpdatesAreVisible(type);
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesLowerCaseIdentifiers();
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesLowerCaseQuotedIdentifiers();
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesMixedCaseIdentifiers();
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesMixedCaseQuotedIdentifiers();
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesUpperCaseIdentifiers();
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return throwExceptionDelegate.storesUpperCaseQuotedIdentifiers();
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return throwExceptionDelegate.supportsAlterTableWithAddColumn();
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return throwExceptionDelegate.supportsAlterTableWithDropColumn();
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return throwExceptionDelegate.supportsANSI92EntryLevelSQL();
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    return throwExceptionDelegate.supportsANSI92FullSQL();
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return throwExceptionDelegate.supportsANSI92IntermediateSQL();
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return throwExceptionDelegate.supportsBatchUpdates();
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return throwExceptionDelegate.supportsCatalogsInDataManipulation();
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsCatalogsInIndexDefinitions();
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsCatalogsInPrivilegeDefinitions();
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return throwExceptionDelegate.supportsCatalogsInProcedureCalls();
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsCatalogsInTableDefinitions();
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return throwExceptionDelegate.supportsColumnAliasing();
  }

  public boolean supportsConvert() throws SQLException {
    return throwExceptionDelegate.supportsConvert();
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return throwExceptionDelegate.supportsConvert(fromType, toType);
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    return throwExceptionDelegate.supportsCoreSQLGrammar();
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return throwExceptionDelegate.supportsCorrelatedSubqueries();
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return throwExceptionDelegate.supportsDataDefinitionAndDataManipulationTransactions();
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return throwExceptionDelegate.supportsDataManipulationTransactionsOnly();
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return throwExceptionDelegate.supportsDifferentTableCorrelationNames();
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return throwExceptionDelegate.supportsExpressionsInOrderBy();
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return throwExceptionDelegate.supportsExtendedSQLGrammar();
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return throwExceptionDelegate.supportsFullOuterJoins();
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    return throwExceptionDelegate.supportsGetGeneratedKeys();
  }

  public boolean supportsGroupBy() throws SQLException {
    return throwExceptionDelegate.supportsGroupBy();
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return throwExceptionDelegate.supportsGroupByBeyondSelect();
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    return throwExceptionDelegate.supportsGroupByUnrelated();
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return throwExceptionDelegate.supportsIntegrityEnhancementFacility();
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    return throwExceptionDelegate.supportsLikeEscapeClause();
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return throwExceptionDelegate.supportsLimitedOuterJoins();
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return throwExceptionDelegate.supportsMinimumSQLGrammar();
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return throwExceptionDelegate.supportsMixedCaseIdentifiers();
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return throwExceptionDelegate.supportsMixedCaseQuotedIdentifiers();
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return throwExceptionDelegate.supportsMultipleOpenResults();
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return throwExceptionDelegate.supportsMultipleResultSets();
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return throwExceptionDelegate.supportsMultipleTransactions();
  }

  public boolean supportsNamedParameters() throws SQLException {
    return throwExceptionDelegate.supportsNamedParameters();
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return throwExceptionDelegate.supportsNonNullableColumns();
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return throwExceptionDelegate.supportsOpenCursorsAcrossCommit();
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return throwExceptionDelegate.supportsOpenCursorsAcrossRollback();
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return throwExceptionDelegate.supportsOpenStatementsAcrossCommit();
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return throwExceptionDelegate.supportsOpenStatementsAcrossRollback();
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    return throwExceptionDelegate.supportsOrderByUnrelated();
  }

  public boolean supportsOuterJoins() throws SQLException {
    return throwExceptionDelegate.supportsOuterJoins();
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return throwExceptionDelegate.supportsPositionedDelete();
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return throwExceptionDelegate.supportsPositionedUpdate();
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return throwExceptionDelegate.supportsResultSetConcurrency(type, concurrency);
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return throwExceptionDelegate.supportsResultSetHoldability(holdability);
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return throwExceptionDelegate.supportsResultSetType(type);
  }

  public boolean supportsSavepoints() throws SQLException {
    return throwExceptionDelegate.supportsSavepoints();
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return throwExceptionDelegate.supportsSchemasInDataManipulation();
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsSchemasInIndexDefinitions();
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsSchemasInPrivilegeDefinitions();
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return throwExceptionDelegate.supportsSchemasInProcedureCalls();
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return throwExceptionDelegate.supportsSchemasInTableDefinitions();
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return throwExceptionDelegate.supportsSelectForUpdate();
  }

  public boolean supportsStatementPooling() throws SQLException {
    return throwExceptionDelegate.supportsStatementPooling();
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return throwExceptionDelegate.supportsStoredFunctionsUsingCallSyntax();
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return throwExceptionDelegate.supportsStoredProcedures();
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return throwExceptionDelegate.supportsSubqueriesInComparisons();
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return throwExceptionDelegate.supportsSubqueriesInExists();
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return throwExceptionDelegate.supportsSubqueriesInIns();
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return throwExceptionDelegate.supportsSubqueriesInQuantifieds();
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return throwExceptionDelegate.supportsTableCorrelationNames();
  }

  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return throwExceptionDelegate.supportsTransactionIsolationLevel(level);
  }

  public boolean supportsTransactions() throws SQLException {
    return throwExceptionDelegate.supportsTransactions();
  }

  public boolean supportsUnion() throws SQLException {
    return throwExceptionDelegate.supportsUnion();
  }

  public boolean supportsUnionAll() throws SQLException {
    return throwExceptionDelegate.supportsUnionAll();
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return throwExceptionDelegate.unwrap(iface);
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    return throwExceptionDelegate.updatesAreDetected(type);
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    return throwExceptionDelegate.usesLocalFilePerTable();
  }

  public boolean usesLocalFiles() throws SQLException {
    return throwExceptionDelegate.usesLocalFiles();
  }

  // java 7

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return throwExceptionDelegate.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return throwExceptionDelegate.generatedKeyAlwaysReturned();
  }
}
