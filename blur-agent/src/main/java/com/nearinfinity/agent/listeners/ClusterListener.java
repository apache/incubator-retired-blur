package com.nearinfinity.agent.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public class ClusterListener implements Watcher {
	private ZooKeeper zk;
	private int zkInstanceId;
	private JdbcTemplate jdbc;
	
	public ClusterListener(ZooKeeper zk, int zkInstanceId, final JdbcTemplate jdbc) {
		this.zk = zk;
		this.zkInstanceId = zkInstanceId;
		this.jdbc = jdbc;
		
		setupWatcher(this);
	}
	
	private void setupWatcher(final Watcher watcher) {
		new Thread(new Runnable(){
			@Override
			public void run() {
				while (true) {
					updateClusters();
					
					synchronized (watcher) {
						try {
							watcher.wait();
						} catch (InterruptedException e) {
							System.out.println("Exiting Cluster listener");
							return;
						}
						System.out.println("Cluster listener was woken up, time to do work");
					}
				}
			}
		}).start();
	}
	
	private void updateClusters() {
		List<String> onlineClusters = getClusters();
		for (String cluster : onlineClusters) {
			//TODO: get uri
			initializeCluster(cluster, "");
		}
	}
	
	private List<String> getClusters() {
		try {
			return zk.getChildren("/blur", this);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	private void initializeCluster(String name, String uri) {
		List<Map<String, Object>> instances = jdbc.queryForList("select id from clusters where name = ?", new Object[]{name});
		if (instances.isEmpty()) {
			jdbc.update("insert into clusters (name, zookeeper_id) values (?, ?)", new Object[]{name, zkInstanceId});
			int clusterId = jdbc.queryForInt("select id from clusters where name = ?", new Object[]{name});
			
			new ShardsListener(zk, clusterId, name, jdbc);
			//TODO: Setup shards here
		} else {
			//Not sure if there is anything to update here
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
		
		synchronized (this) {
			notifyAll();
		}
	}

}
