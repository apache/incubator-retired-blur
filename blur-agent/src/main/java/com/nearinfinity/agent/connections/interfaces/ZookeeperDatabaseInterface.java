package com.nearinfinity.agent.connections.interfaces;

public interface ZookeeperDatabaseInterface {

  int getZookeeperId(String name);

  void setZookeeperOnline(int id);
  
  void setZookeeperOffline(int id);
  
  int insertOrUpdateCluster(boolean safeMode, String cluster, int zookeeperId);
}
