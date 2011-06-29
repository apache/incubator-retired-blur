package com.nearinfinity.agent.listeners;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public class ShardListener implements Watcher {
	private ZooKeeper zk;
	private int clusterId;
	private JdbcTemplate jdbc;
	
	public ShardListener(ZooKeeper zk, int clusterId, final JdbcTemplate jdbc) {
		this.zk = zk;
		this.clusterId = clusterId;
		this.jdbc = jdbc;
		
		setupWatcher(this);
	}
	
	private void setupWatcher(final Watcher watcher) {
		new Thread(new Runnable(){
			@Override
			public void run() {
				while (true) {
					updateShards();
					
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
	
	private void updateShards() {
		List<String> onlineClusters = getShards();
		for (String cluster : onlineClusters) {
			//TODO: get uri
			initializeShard(cluster, "");
		}
	}
	
	public List<String> getShards() {
		return null;
	}
	
	public void initializeShard(String name, String whatever) {
		
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

}
