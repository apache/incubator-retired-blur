package com.nearinfinity.agent.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZookeeperWatcher implements Watcher {
	private InstanceManager manager;
	
	public ZookeeperWatcher(InstanceManager manager) {
		this.manager = manager;
	}
	
	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		
		if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the connection has changed
            switch (event.getState()) {
	            case SyncConnected:
	                // In this particular example we don't need to do anything
	                // here - watches are automatically re-registered with 
	                // server and any watches triggered while the client was 
	                // disconnected will be delivered (in order of course)
	                break;
	            case Expired:
	                // It's all over
	            	manager.resetConnection();
	            	synchronized (this) {
	            		notify();
					}
	                break;
            }
        } else if (path != null){
        	// Something in Zookeeper has changed, need to update the models
        	System.out.println("Something changed: " + path);
        }
	}

}
