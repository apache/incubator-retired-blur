package com.nearinfinity.agent.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.agent.zookeeper.collectors.ClusterCollector;
import com.nearinfinity.agent.zookeeper.collectors.ControllerCollector;

public class ZookeeperWatcher implements Watcher {
	private InstanceManager manager;
	
	private static final Log log = LogFactory.getLog(ZookeeperWatcher.class);

	public ZookeeperWatcher(InstanceManager manager) {
		this.manager = manager;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			String path = event.getPath();
			ZooKeeper zk = manager.getInstance();
			switch (event.getType()) {
			case None:
				// We are are being told that the state of the connection has
				// changed
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
				break;
			case NodeChildrenChanged:
				nodeChildrenChanged(path, zk);
				break;
			case NodeCreated:
				nodeCreated(path, zk);
				break;
			case NodeDataChanged:
				nodeChanged(path, zk);
				break;
			case NodeDeleted:
				nodeDeleted(path, zk);
				break;
			}
		} catch (KeeperException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		}
	}

	private void nodeDeleted(String path, ZooKeeper zk) {
		recollectEverything();
	}

	private void nodeChanged(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		zk.getData(path, true, null);
		recollectEverything();
	}

	private void nodeCreated(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		zk.getData(path, true, null);
		recollectEverything();
	}

	private void nodeChildrenChanged(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		zk.getChildren(path, true);
		recollectEverything();
	}
	
	private void recollectEverything() {
		ControllerCollector.collect(manager);
		ClusterCollector.collect(manager);
	}

}
