package com.nearinfinity.agent.connections.interfaces;

public interface TableDatabaseInterface {
  void updateTableSchema(final String tableName, final Integer clusterId, final String schema,
      final String tableAnalyzer);

  void updateTableServer(final String tableName, final Integer clusterId, final String server);

  void updateTableStats(final String tableName, final Integer clusterId, final Long tableBytes,
      final Long tableQueries, final Long tableRecordCount, final Long tableRowCount);
}