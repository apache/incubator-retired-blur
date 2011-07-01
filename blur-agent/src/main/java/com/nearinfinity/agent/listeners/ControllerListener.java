package com.nearinfinity.agent.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public class ControllerListener implements Watcher {
	private ZooKeeper zk;
	private String clusterName;
	private int zkInstanceId;
	private JdbcTemplate jdbc;
	
	public ControllerListener(ZooKeeper zk, final String clusterName, int zkInstanceId, final JdbcTemplate jdbc) {
		this.zk = zk;
		this.clusterName = clusterName;
		this.zkInstanceId = zkInstanceId;
		this.jdbc = jdbc;
		
		setupWatcher(this);
	}
	
	private void setupWatcher(final Watcher watcher) {
		new Thread(new Runnable(){
			@Override
			public void run() {
				while (true) {
					updateControllers();
					
					synchronized (watcher) {
						try {
							watcher.wait();
						} catch (InterruptedException e) {
							System.out.println("Exiting Controller listener");
							return;
						}
						System.out.println("Controller listener was woken up, time to do work");
					}
				}
			}
		}).start();
	}
	
	private void updateControllers() {
		List<String> onlineControllers = getControllers();
		markOfflineControllers(onlineControllers);
		addNewControllers(onlineControllers);
	}
	
	private void markOfflineControllers(List<String> controllers) {
		if (controllers.isEmpty()) {
			jdbc.update("update controllers set status = 0");
		} else {
			jdbc.update("update controllers set status = 0 where node_name not in ('" + StringUtils.join(controllers, "','") + "')");
			
			for (String controller : controllers) {
				initializeController(controller, "");
			}
		}
	}
	
	private void addNewControllers(List<String> controllers) {
		for (String controller : controllers) {
			//TODO: get uri
			initializeController(controller, "");
		}
	}
	
	private List<String> getControllers() {
		try {
			return zk.getChildren("/blur/" + clusterName + "/online/controller-nodes", this);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	private void initializeController(String name, String uri) {
		List<Map<String, Object>> instances = jdbc.queryForList("select id from controllers where node_name = ?", new Object[]{name});
		if (instances.isEmpty()) {
			jdbc.update("insert into controllers (node_name, node_location, status, zookeeper_id, blur_version) values (?, ?, ?, ?, ?)", new Object[]{name, uri, 1, zkInstanceId, "1.0"});
		} else {
			jdbc.update("update controllers set status=1, blur_version=? where node_name=?", new Object[]{"1.0", name});
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
