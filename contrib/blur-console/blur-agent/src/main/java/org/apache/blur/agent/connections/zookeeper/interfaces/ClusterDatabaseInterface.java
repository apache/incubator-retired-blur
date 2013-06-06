package org.apache.blur.agent.connections.zookeeper.interfaces;

public interface ClusterDatabaseInterface extends ShardsDatabaseInterface, TableDatabaseInterface {

	int insertOrUpdateCluster(boolean safeMode, String cluster, int zookeeperId);

}
