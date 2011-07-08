package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ClusterCollector {
	private ZooKeeper zk;
	private int instanceId;
	private JdbcTemplate jdbc;
	private InstanceManager manager;
	
	private ClusterCollector(InstanceManager manager, JdbcTemplate jdbc) {
		this.zk = manager.getInstance();
		this.instanceId = manager.getInstanceId();
		this.jdbc = jdbc;
		this.manager = manager;
		
		updateClusters();
	}
	
	private void updateClusters() {
		List<String> onlineClusters = getClusters();
		updateOnlineClusters(onlineClusters);
	}
	
	private void updateOnlineClusters(List<String> clusters) {
		for (String cluster : clusters) {
			List<Map<String, Object>> instances = jdbc.queryForList("select id from clusters where name = ?", new Object[]{cluster});
			int clusterId;
			if (instances.isEmpty()) {
				jdbc.update("insert into clusters (name, zookeeper_id) values (?, ?)", new Object[]{cluster, instanceId});
				clusterId = jdbc.queryForInt("select id from clusters where name = ?", new Object[]{cluster});
			} else {
				clusterId = (Integer) instances.get(0).get("ID");
			}
			
			ShardCollector.collect(manager, jdbc, clusterId, cluster);
			TableCollector.collect(manager, jdbc, clusterId, cluster);
		}
	}
	
	private List<String> getClusters() {
		try {
			return zk.getChildren("/blur", false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	public static void collect(InstanceManager manager, JdbcTemplate jdbc) {
		new ClusterCollector(manager, jdbc);
	}
}
