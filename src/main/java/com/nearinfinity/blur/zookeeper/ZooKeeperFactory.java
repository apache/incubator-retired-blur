package com.nearinfinity.blur.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperFactory {
	private static final Log LOG = LogFactory.getLog(ZooKeeperFactory.class);
	private static ZooKeeper zk;
	//@todo
	private static String connectString = "localhost";
	private static int sessionTimeout = 3000;

	public static synchronized ZooKeeper getZooKeeper() throws IOException {
		if (zk == null) {
			init();
		}
		return zk;
	}

	private static void init() throws IOException {
		LOG.info("Connecting to zookeeper [" + connectString + "] with [" + sessionTimeout + "] timeout");
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				//do nothing
			}
		});
	}

}
