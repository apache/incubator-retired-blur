package com.nearinfinity.agent.collectors;

import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.types.InstanceManager;

public abstract class Collector {
	InstanceManager manager;

	protected Collector(InstanceManager manager) {
		this.manager = manager;
	}

	protected ZooKeeper getZk() {
		return manager.getInstance();
	}

	protected JdbcTemplate getJdbc() {
		return manager.getJdbc();
	}

}
