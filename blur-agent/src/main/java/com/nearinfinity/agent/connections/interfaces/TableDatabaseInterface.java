package com.nearinfinity.agent.connections.interfaces;

import java.util.List;
import java.util.Map;

import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;

public interface TableDatabaseInterface {
  String getZookeeperId(final String zookeeperName) throws ZookeeperNameMissingException,
      ZookeeperNameCollisionException;

  List<Map<String, Object>> getClusters(final String zookeeperId);

  Map<String, Object> getExistingTable(final String table, final Integer clusterId)
      throws TableMissingException, TableCollisionException;

  // Update methods
  void updateTableSchema(final String tableName, final Integer clusterId, final String schema,
      final String tableAnalyzer);

  void updateTableServer(final String tableName, final Integer clusterId, final String server);

  void updateTableStats(final String tableName, final Integer clusterId, final Long tableBytes,
      final Long tableQueries, final Long tableRecordCount, final Long tableRowCount);
}