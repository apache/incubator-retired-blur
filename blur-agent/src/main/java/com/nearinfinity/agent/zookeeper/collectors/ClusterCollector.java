package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ClusterCollector {
	private ZooKeeper zk;
	private int instanceId;
	private JdbcTemplate jdbc;
	private InstanceManager manager;
	
	private static final Log log = LogFactory.getLog(ClusterCollector.class);
	
	private ClusterCollector(InstanceManager manager) {
		this.zk = manager.getInstance();
		this.instanceId = manager.getInstanceId();
		this.jdbc = manager.getJdbc();
		this.manager = manager;
		
		updateClusters();
	}
	
	private void updateClusters() {
		List<String> onlineClusters = getClusters();
		updateOnlineClusters(onlineClusters);
	}
	
	private void updateOnlineClusters(List<String> clusters) {
		for (String cluster : clusters) {
			List<Map<String, Object>> instances = jdbc.queryForList("select id from clusters where name = ? and zookeeper_id=?", cluster, instanceId);
			int clusterId;
			if (instances.isEmpty()) {
				jdbc.update("insert into clusters (name, zookeeper_id) values (?, ?)", cluster, instanceId);
				clusterId = jdbc.queryForInt("select id from clusters where name = ? and zookeeper_id=?", cluster, instanceId);
			} else {
				clusterId = (Integer) instances.get(0).get("ID");
			}
			
			ShardCollector.collect(manager, jdbc, clusterId, cluster);
			TableCollector.collect(manager, jdbc, clusterId, cluster);
		}
	}
	
	private List<String> getClusters() {
		try {
			return zk.getChildren("/blur/clusters", true);
		} catch (KeeperException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		}
		return new ArrayList<String>();
	}

	public static void collect(InstanceManager manager) {
		new ClusterCollector(manager);
	}
}
