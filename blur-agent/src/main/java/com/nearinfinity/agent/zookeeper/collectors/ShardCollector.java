package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ShardCollector {
	private ZooKeeper zk;
	private int clusterId;
	private String clusterName;
	private JdbcTemplate jdbc;
	
	private ShardCollector(InstanceManager manager, JdbcTemplate jdbc, int clusterId, String clusterName) {
		this.zk = manager.getInstance();
		this.clusterId = clusterId;
		this.clusterName = clusterName;
		this.jdbc = jdbc;
		
		updateShards();
	}
	
	private void updateShards() {
		List<String> shards = getShards();
		markOfflineShards(shards);
		updateOnlineShards(shards);
	}
	
	private List<String> getShards() {
		try {
			return zk.getChildren("/blur/" + clusterName + "/online/shard-nodes", false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	private void markOfflineShards(List<String> shards) {
		if (shards.isEmpty()) {
			jdbc.update("update shards set status = 0 where cluster_id = ?", new Object[]{clusterId});
		} else {
			jdbc.update("update shards set status = 0 where node_name not in ('" + StringUtils.join(shards, "','") + "') and cluster_id=?", new Object[]{clusterId});
		}
	}
	
	private void updateOnlineShards(List<String> shards) {
		for (String shard : shards) {
			List<Map<String, Object>> instances = jdbc.queryForList("select id from shards where node_name = ?", new Object[]{shard});
			if (instances.isEmpty()) {
				jdbc.update("insert into shards (node_name, node_location, status, cluster_id, blur_version) values (?, ?, ?, ?, ?)", new Object[]{shard, "placeholder", 2, clusterId, "1.0"});
			} else {
				jdbc.update("update shards set status=2, blur_version=? where node_name=? and cluster_id=?", new Object[]{"1.0", shard, clusterId});
			}
		}
	}

	public static void collect(InstanceManager manager, JdbcTemplate jdbc, int clusterId, String clusterName) {
		new ShardCollector(manager, jdbc, clusterId, clusterName);
	}
}
