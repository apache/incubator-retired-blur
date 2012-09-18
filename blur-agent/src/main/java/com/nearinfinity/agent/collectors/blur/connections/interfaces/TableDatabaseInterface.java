package com.nearinfinity.agent.collectors.blur.connections.interfaces;

import java.util.List;
import java.util.Map;

import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;

public interface TableDatabaseInterface {
  String getTableId(String zookeeperName) throws ZookeeperNameMissingException, ZookeeperNameCollisionException;
  List<Map<String, Object>> getClusters(String zookeeperId);
  Map<String, Object> getExistingTable(String table, Integer clusterId) throws TableMissingException, TableCollisionException;
  void updateExistingTable(Object[] properties);
}
