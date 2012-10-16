package com.nearinfinity.agent.connections;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.TableDatabaseInterface;

public class TableDatabaseConnection implements TableDatabaseInterface {

  private final JdbcTemplate jdbc;

  public TableDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public int getTableId(int clusterId, String tableName) {
    return jdbc.queryForInt("select id from blur_tables where cluster_id=? and table_name =?", clusterId, tableName);
  }
  
  @Override
  public void updateTableSchema(final int tableId, final String schema, final String tableAnalyzer) {
    jdbc.update("update blur_tables set table_schema=?, table_analyzer=? where id=?", new Object[] {
        schema, tableAnalyzer, tableId });
  }

  @Override
  public void updateTableServer(final int tableId, String server) {
    jdbc.update("update blur_tables set server=? where id=?", new Object[] { server, tableId });
  }

  @Override
  public void updateTableStats(final int tableId, Long tableBytes, Long tableQueries,
      Long tableRecordCount, Long tableRowCount) {
    jdbc.update(
        "update blur_tables set current_size=?, query_usage=?, record_count=?, row_count=? where id=?",
        new Object[] { tableBytes, tableQueries, tableRecordCount, tableRowCount, tableId });
  }
}
