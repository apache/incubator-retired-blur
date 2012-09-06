package com.nearinfinity.agent.types;

import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public interface InstanceManager {
	void resetConnection();
	ZooKeeper getInstance();
	int getInstanceId();
	JdbcTemplate getJdbc();
}
