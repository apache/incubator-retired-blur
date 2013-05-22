package org.apache.blur.agent.connections.zookeeper.interfaces;

import java.util.List;

public interface ShardsDatabaseInterface {

	int markOfflineShards(List<String> onlineShards, int clusterId);

	void updateOnlineShard(String shard, int clusterId, String blurVersion);

	List<String> getRecentOfflineShardNames(int amount);

}
