package com.nearinfinity.agent.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.collectors.ClusterCollector;
import com.nearinfinity.agent.zookeeper.collectors.ControllerCollector;

public class ZookeeperWatcher implements Watcher {

//	public static void main(String[] args) throws Exception {
//		InstanceManager manager = new InstanceManager() {
//			private ZooKeeper zk;
//			@Override
//			public void resetConnection() {
//				zk = null;
//			}
//
//			@Override
//			public int getInstanceId() {
//				return 0;
//			}
//
//			@Override
//			public ZooKeeper getInstance() {
//				try {
//					if(zk == null) {
//						Watcher watcher = new ZookeeperWatcher(this);
//						String url = "localhost";
//						zk = new ZooKeeper(url, 3000, watcher);
//					}
//					return zk;
//				} catch (IOException e) {
//					e.printStackTrace();
//					return null;
//				}
//
//			}
//		};
//		ZooKeeper zooKeeper = manager.getInstance();
//		byte[] data = zooKeeper.getData("/aatest", true, null);
//		zooKeeper.getChildren("/", true);
//		System.out.println(zooKeeper.exists("/crtest", true));
//		System.out.println(new String(data));
//		while(true){
//			Thread.sleep(5000);
//		}
//	}

	private InstanceManager manager;

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
					System.out.println("Zookeeper expired");
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void nodeDeleted(String path, ZooKeeper zk) {
		System.out.println("node deleted " + path);
		recollectEverything();
	}

	private void nodeChanged(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		byte[] data = zk.getData(path, true, null);
		System.out.println("node changed " + path);
		recollectEverything();
	}

	private void nodeCreated(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		byte[] data = zk.getData(path, true, null);
		System.out.println("node created " + path);
		recollectEverything();
	}

	private void nodeChildrenChanged(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(path, true);
		System.out.println("node children changed for " + path);
		recollectEverything();
	}
	
	private void recollectEverything() {
		ControllerCollector.collect(manager);
		ClusterCollector.collect(manager);
	}

}
