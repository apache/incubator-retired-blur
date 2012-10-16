package com.nearinfinity.agent.connections.interfaces;

import java.util.List;

public interface ZookeeperDatabaseInterface {

  void setZookeeperOnline(int id);

  void setZookeeperOffline(int id);
  
  int insertOrUpdateZookeeper(String name, String url, String blurConnection);

  int insertOrUpdateCluster(boolean safeMode, String cluster, int zookeeperId);

  void markOfflineControllers(List<String> onlineControllers, int zookeeperId);
  
  void markOfflineShards(List<String> onlineShards, int clusterId);
  
  void markOfflineTables(List<String> onlineTables, int clusterId);

  void updateOnlineController(String controller, int zookeeperId, String blurVersion);

  void updateOnlineShard(String shard, int clusterId, String blurVersion);

  void updateOnlineTable(String table, int clusterId, String uri, boolean enabled);
}
