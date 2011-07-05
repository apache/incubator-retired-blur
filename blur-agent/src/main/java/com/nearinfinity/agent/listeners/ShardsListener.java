package com.nearinfinity.agent.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public class ShardsListener implements Watcher {
	private ZooKeeper zk;
	private int clusterId;
	private String clusterName;
	private JdbcTemplate jdbc;
	
	public ShardsListener(ZooKeeper zk, int clusterId, String clusterName, final JdbcTemplate jdbc) {
		this.zk = zk;
		this.clusterId = clusterId;
		this.clusterName = clusterName;
		this.jdbc = jdbc;
		
		runInitalShardChecker();
		setupWatcher(this);
	}
	
	private void runInitalShardChecker() {
		List<String> shards = getShards(null);
		
		for (String shard : shards) {
			initializeShard(shard);
		}
	}
	
	private void setupWatcher(final Watcher watcher) {
		new Thread(new Runnable(){
			@Override
			public void run() {
				while (true) {
					updateShards(watcher);
					
					synchronized (watcher) {
						try {
							watcher.wait();
						} catch (InterruptedException e) {
							System.out.println("Exiting Shard listener");
							return;
						}
						System.out.println("Shard listener was woken up, time to do work");
					}
				}
			}
		}).start();
	}
	
	private void updateShards(Watcher watcher) {
		List<String> onlineShards = getShards(watcher);
		for (String shard : onlineShards) {
			//TODO: get uri
			initializeShard(shard);
		}
	}
	
	public List<String> getShards(Watcher watcher) {
		String path;
		if (watcher == null) {
			path = "/blur/" + clusterName + "/shard-nodes";
		} else {
			path = "/blur/" + clusterName + "/online/shard-nodes";
		}
		
		try {
			return zk.getChildren(path, watcher);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	public void initializeShard(String name) {
		List<Map<String, Object>> instances = jdbc.queryForList("select id from shards where node_name = ?", new Object[]{name});
		if (instances.isEmpty()) {
			jdbc.update("insert into shards (node_name, node_location, status, cluster_id, blur_version) values (?, ?, ?, ?, ?)", new Object[]{name, "", 1, clusterId, "1.0"});
		} else {
			jdbc.update("update shards set status=1, blur_version=? where node_name=?", new Object[]{"1.0", name});
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

}
