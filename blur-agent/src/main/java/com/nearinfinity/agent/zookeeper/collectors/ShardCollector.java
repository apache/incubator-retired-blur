package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;

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
			return zk.getChildren("/blur/clusters/" + clusterName + "/online/shard-nodes", true);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	private void markOfflineShards(List<String> shards) {
		if (shards.isEmpty()) {
			jdbc.update("update shards set status = 0 where cluster_id = ?", clusterId);
		} else {
			jdbc.update("update shards set status = 0 where node_name not in ('" + StringUtils.join(shards, "','") + "') and cluster_id=?", clusterId);
		}
	}
	
	private void updateOnlineShards(List<String> shards) {
		for (String shard : shards) {
			int status = 1;
			String blurVersion = "1.0";			
			
			int updatedCount = jdbc.update("update shards set status=1, blur_version=? where node_name=? and cluster_id=?", blurVersion, shard, clusterId);
			
			if (updatedCount == 0) {
				jdbc.update("insert into shards (node_name, status, cluster_id, blur_version) values (?, 1, ?, ?)", shard, clusterId, blurVersion);				
			}
		}
	}

	public static void collect(InstanceManager manager, JdbcTemplate jdbc, int clusterId, String clusterName) {
		new ShardCollector(manager, jdbc, clusterId, clusterName);
	}
}
