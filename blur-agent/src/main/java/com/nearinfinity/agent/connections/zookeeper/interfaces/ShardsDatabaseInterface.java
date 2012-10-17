package com.nearinfinity.agent.connections.zookeeper.interfaces;

import java.util.List;

public interface ShardsDatabaseInterface {

  void markOfflineShards(List<String> onlineShards, int clusterId);

  void updateOnlineShard(String shard, int clusterId, String blurVersion);

}
