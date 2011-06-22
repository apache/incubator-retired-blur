package com.nearinfinity.agent.listeners;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ControllerListener implements Watcher {
	private ZooKeeper zk;
	private String clusterName;
	
	public ControllerListener(ZooKeeper zk, String clusterName) {
		this.zk = zk;
		this.clusterName = clusterName;
		
		try {
			List<String> children = zk.getChildren("/blur/" + clusterName + "/online/controller-nodes", false);
			
			System.out.println(children);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void process(WatchedEvent event) {

	}
}
