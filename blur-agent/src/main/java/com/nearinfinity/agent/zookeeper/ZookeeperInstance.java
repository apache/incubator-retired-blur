package com.nearinfinity.agent.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.listeners.ClusterListener;
import com.nearinfinity.agent.listeners.ControllerListener;

public class ZookeeperInstance implements Watcher {
	private String name;
	private String url;
	private JdbcTemplate jdbc;
	private ZooKeeper zk;
	
	public ZookeeperInstance(String name, String url, JdbcTemplate jdbc) {
		this.name = name;
		this.url = url;
		this.jdbc = jdbc;
		setupWatcher(this);
	}
	
	private void setupWatcher(final Watcher watcher) {
		new Thread(new Runnable(){
			@Override
			public void run() {
				while (true) {
					boolean online = true;
					try {
						zk = new ZooKeeper(url, 3000, watcher);
					} catch (IOException e) {
						online = false;
					}
					
					int instanceId = initializeZkInstanceModel(name, url, jdbc, online);
					
					if (online) {
						// TODO: Name will not be needed in the future
						new ControllerListener(zk, name, instanceId, jdbc);
						new ClusterListener(zk, instanceId, jdbc);
						
						try {
							synchronized (watcher) {
								watcher.wait();
								System.out.println("Zookeeper Watcher was woken up, time to do work.");
							}
						} catch (InterruptedException e) {
							System.out.println("Exiting Zookeeper instance");
							return;
						}
					} else {
						System.out.println("Instance is not online.  Going to sleep for 30 seconds and try again.");
						try {
							Thread.sleep(30000);
						} catch (InterruptedException e) {
							System.out.println("Exiting Zookeeper instance");
							return;
						}
					}
				}
			}
		}).start();
	}
	
	
	private int initializeZkInstanceModel(String name, String url, JdbcTemplate jdbc, boolean online) {
		List<Map<String, Object>> instances = jdbc.queryForList("select id from blur_zookeeper_instances where name = ?", new Object[]{name});
		if (instances.isEmpty()) {
			jdbc.update("insert into blur_zookeeper_instances (name, url, status) values (?, ?,?)", new Object[]{name, url, 1});
			return jdbc.queryForInt("select id from blur_zookeeper_instances where name = ?", new Object[]{name});
		} else {
			// TODO: Determine if we want to allow changing of the host and port here
			jdbc.update("update blur_zookeeper_instances set status=? where name=?", new Object[]{online ? 0 : 1, name});
		}
		return (Integer) instances.get(0).get("ID");
	}
	
	@Override
	public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
	            case SyncConnected:
	                // In this particular example we don't need to do anything
	                // here - watches are automatically re-registered with 
	                // server and any watches triggered while the client was 
	                // disconnected will be delivered (in order of course)
	                break;
	            case Expired:
	                // It's all over
	            	synchronized (this) {
	            		notify();
					}
	                break;
            }
        }
	}

}
