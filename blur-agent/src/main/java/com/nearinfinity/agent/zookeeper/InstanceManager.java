package com.nearinfinity.agent.zookeeper;

import org.apache.zookeeper.ZooKeeper;

public interface InstanceManager {
	void resetConnection();
	ZooKeeper getInstance();
	int getInstanceId();
}
