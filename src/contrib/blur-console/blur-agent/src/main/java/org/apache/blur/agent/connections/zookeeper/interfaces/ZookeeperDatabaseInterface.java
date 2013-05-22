package org.apache.blur.agent.connections.zookeeper.interfaces;

public interface ZookeeperDatabaseInterface extends ControllerDatabaseInterface, ClusterDatabaseInterface {

	void setZookeeperOnline(int id);

	void setZookeeperWarning(int id);

	void setZookeeperOffline(int id);

  void setZookeeperFailure(int id);

	int insertOrUpdateZookeeper(String name, String url, String blurConnection);

	void setOnlineEnsembleNodes(String ensembleArray, int zookeeperId);

}
