package com.nearinfinity.agent.collectors.zookeeper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.types.InstanceManager;
import com.nearinfinity.agent.types.ZookeeperConnection;

public class ZookeeperInstance implements Runnable {
  private ZookeeperConnection connection;
  
	private static final Log log = LogFactory.getLog(ZookeeperInstance.class);

	public ZookeeperInstance(String name, String url, JdbcTemplate jdbc, Properties props) {
	  this.connection = new ZookeeperConnection(name, url, jdbc, props);
	}

	@Override
	public void run() {
		CountDownLatch latch = new CountDownLatch(1);
		while (true) {
			if (connection.zk == null) {
				try {
					latch = new CountDownLatch(1);
					final CountDownLatch watcherLatch = latch;
					zk = new ZooKeeper(connection.url, 3000, new Watcher() {

						@Override
						public void process(WatchedEvent event) {
							KeeperState state = event.getState();
							if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
								log.warn("zookeeper disconnected event");
								connection.closeZookeeper();
							} else if (state == KeeperState.SyncConnected) {
								watcherLatch.countDown();
								log.info("zk session established");
							}
						}
					});
				} catch (IOException e) {
				  connection.closeZookeeper();
				}
			}

			if (zk == null) {
				log.info("Instance is not online.  Going to sleep for 30 seconds and try again.");
				connection.updateZookeeperStatus(false);
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e) {
					log.info("Exiting Zookeeper instance");
					return;
				}
			} else {
				try {
					if (latch.await(10, TimeUnit.SECONDS)) {
					  connection.updateZookeeperStatus(true);
					} else {
					  connection.closeZookeeper();
						connection.updateZookeeperStatus(false);
					}
				} catch (InterruptedException e1) {
				  connection.closeZookeeper();
				  connection.updateZookeeperStatus(false);
				}
				if(zk == null){
					log.info("Instance is not online.  Going to sleep for 30 seconds and try again.");
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						// do nothing
					}
				} else {
					try {
						ControllerCollector.collect(this, instanceId);
						ClusterCollector.collect(this, instanceId);
					} catch (KeeperException e) {
						handleZkError(e);
					} catch (InterruptedException e) {
						handleZkError(e);
					}
					try {
						Thread.sleep(10000); // poll every 10 seconds
					} catch (InterruptedException e) {
						log.info("Exiting Zookeeper instance");
						return;
					}
				}
			}
		}
	}

	private void handleZkError(Exception e) {
		connection.closeZookeeper();
		if (!(e instanceof SessionExpiredException || e instanceof SessionMovedException)) {
			log.warn("error talking to zookeeper", e);
		} else {
			log.warn("zookeeper session expired");
		}
	}
}
