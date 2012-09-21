package com.nearinfinity.agent.connections;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.TableDatabaseInterface;

public class TableDatabaseConnection implements TableDatabaseInterface {
  private final JdbcTemplate jdbc;

  public TableDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void updateTableSchema(final String tableName, final Integer clusterId, final String schema, final String tableAnalyzer) {
    jdbc.update("update blur_tables set table_schema=?, table_analyzer=? where table_name=? and cluster_id=?",
        new Object[] { schema, tableAnalyzer, tableName, clusterId });
  }

  @Override
  public void updateTableServer(String tableName, Integer clusterId, String server) {
    jdbc.update("update blur_tables set server=? where table_name=? and cluster_id=?",
        new Object[] { server, tableName, clusterId });
  }

  @Override
  public void updateTableStats(String tableName, Integer clusterId, Long tableBytes,
      Long tableQueries, Long tableRecordCount, Long tableRowCount) {
    jdbc.update(
        "update blur_tables set current_size=?, query_usage=?, record_count=?, row_count=? where table_name=? and cluster_id=?",
        new Object[] { tableBytes, tableQueries, tableRecordCount, tableRowCount, tableName, clusterId });
  }
}
