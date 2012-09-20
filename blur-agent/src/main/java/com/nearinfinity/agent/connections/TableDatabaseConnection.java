package com.nearinfinity.agent.connections;

import java.util.Map;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;

public class TableDatabaseConnection implements TableDatabaseInterface {
  private final JdbcTemplate jdbc;

  public TableDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public String getZookeeperId(final String zookeeperName) throws ZookeeperNameMissingException,
      ZookeeperNameCollisionException {
    List<Map<String, Object>> zookeepers = jdbc.queryForList(
        "select id from zookeepers where name = ?", zookeeperName);
    switch (zookeepers.size()) {
    case 0:
      throw new ZookeeperNameMissingException(zookeeperName);
    case 1:
      return zookeepers.get(0).get("ID").toString();
    default:
      throw new ZookeeperNameCollisionException(zookeepers.size(), zookeeperName);
    }
  }

  @Override
  public List<Map<String, Object>> getClusters(final String zookeeperId) {
    return jdbc.queryForList("select id, name from clusters where zookeeper_id = ?", zookeeperId);
  }

  @Override
  public Map<String, Object> getExistingTable(final String table, final Integer clusterId)
      throws TableMissingException, TableCollisionException {
    List<Map<String, Object>> existingTable = jdbc.queryForList(
        "select id, cluster_id from blur_tables where table_name=? and cluster_id=?", table,
        clusterId);
    switch (existingTable.size()) {
    case 0:
      throw new TableMissingException(table);
    case 1:
      return existingTable.get(0);
    default:
      throw new TableCollisionException(existingTable.size(), table);
    }
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
