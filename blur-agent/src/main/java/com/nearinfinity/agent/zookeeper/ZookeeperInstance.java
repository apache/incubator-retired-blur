package com.nearinfinity.agent.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.collectors.ClusterCollector;
import com.nearinfinity.agent.zookeeper.collectors.ControllerCollector;

public class ZookeeperInstance implements InstanceManager, Runnable {
	private String name;
	private String url;
	private int instanceId;
	private JdbcTemplate jdbc;
	private ZooKeeper zk;
	private Watcher watcher;
	
	public ZookeeperInstance(String name, String url, JdbcTemplate jdbc) {
		this.name = name;
		this.url = url;
		this.jdbc = jdbc;
		
		initializeZkInstanceModel();
		resetConnection();
	}
	
	private void initializeZkInstanceModel() {
		List<Map<String, Object>> instances = jdbc.queryForList("select id from zookeepers where name = ?", new Object[]{name});
		if (instances.isEmpty()) {
			jdbc.update("insert into zookeepers (name, url) values (?, ?)", new Object[]{name, url});
			instanceId = jdbc.queryForInt("select id from zookeepers where name = ?", new Object[]{name});
		} else {
			instanceId = (Integer) instances.get(0).get("ID");
		}
	}

	@Override
	public void resetConnection() {
		zk = null;
	}

	@Override
	public void run() {	
		boolean needsInitialPath = true;
		while(true) {
			if (zk == null) {
				try {
					watcher = new ZookeeperWatcher(this);
					zk = new ZooKeeper(url, 3000, watcher);
				} catch (IOException e) {
					zk = null;
				}
			}
			
			if (zk == null) {
				System.out.println("Instance is not online.  Going to sleep for 30 seconds and try again.");
				updateZookeeperStatus(false);
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e) {
					System.out.println("Exiting Zookeeper instance");
					return;
				}
			} else {
				updateZookeeperStatus(true);
				if (needsInitialPath) {
					runInitialRegistration();
					needsInitialPath = false;
				}
				try {
					synchronized (watcher) {
						watcher.wait();
						System.out.println("Zookeeper Watcher was woken up, time to do work.");
					}
				} catch (InterruptedException e) {
					System.out.println("Exiting Zookeeper instance");
					return;
				}
			}
		}
	}
	
	private void updateZookeeperStatus(boolean online) {
		jdbc.update("update zookeepers set status=? where id=?", (online ? 1 : 0), instanceId);
	}

	@Override
	public ZooKeeper getInstance() {
		return zk;
	}
	
	@Override
	public int getInstanceId() {
		return instanceId;
	}
	
	private void runInitialRegistration() {
		ControllerCollector.collect(this, jdbc);
		ClusterCollector.collect(this, jdbc);
	}

}
