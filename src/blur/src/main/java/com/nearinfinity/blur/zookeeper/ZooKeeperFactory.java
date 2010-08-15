package com.nearinfinity.blur.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperFactory {
	private final static Logger LOG = LoggerFactory.getLogger(ZooKeeperFactory.class);
	private static ZooKeeper zk;
	//@todo get form blur config
	private static String connectString = "localhost";
	private static int sessionTimeout = 3000;

	public static synchronized ZooKeeper getZooKeeper() throws IOException {
		if (zk == null) {
			init();
		}
		return zk;
	}

	private static void init() throws IOException {
		LOG.info("Connecting to zookeeper {} with {} timeout",connectString,sessionTimeout);
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent watchedEvent) {
				//do nothing
			}
		});
	}

}
