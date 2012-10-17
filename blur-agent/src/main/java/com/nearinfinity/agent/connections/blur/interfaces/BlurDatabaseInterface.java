package com.nearinfinity.agent.connections.blur.interfaces;

import java.util.List;
import java.util.Map;

import com.nearinfinity.agent.connections.blur.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;

public interface BlurDatabaseInterface extends TableDatabaseInterface, QueryDatabaseInterface {
  String getConnectionString(String zookeeperName);

  String getZookeeperId(final String zookeeperName) throws ZookeeperNameMissingException,
      ZookeeperNameCollisionException;

  List<Map<String, Object>> getClusters(final int zookeeperId);

  Map<String, Object> getExistingTable(final String table, final Integer clusterId)
      throws TableMissingException, TableCollisionException;

}
