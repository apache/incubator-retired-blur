package com.nearinfinity.agent.types;

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

import com.nearinfinity.agent.collectors.zookeeper.ClusterCollector;
import com.nearinfinity.agent.collectors.zookeeper.ControllerCollector;

public class ZookeeperInstance implements InstanceManager, Runnable {
	private String name;
	private String url;
	private int instanceId;
	private JdbcTemplate jdbc;
	private ZooKeeper zk;
	private Properties props;

	private static final Log log = LogFactory.getLog(ZookeeperInstance.class);

	public ZookeeperInstance(String name, String url, JdbcTemplate jdbc, Properties props) {
		this.name = name;
		this.url = url;
		this.jdbc = jdbc;
		this.props = props;

		initializeZkInstanceModel();
		resetConnection();
	}

	private void initializeZkInstanceModel() {
		String blurConnection = props.getProperty("blur." + name + ".url");

		int updatedCount = jdbc.update("update zookeepers set url=?, blur_urls=? where name=?", url, blurConnection,
				name);

		if (updatedCount == 0) {
			jdbc.update("insert into zookeepers (name, url, blur_urls) values (?, ?, ?)", name, url, blurConnection);
		}

		instanceId = jdbc.queryForInt("select id from zookeepers where name = ?", new Object[] { name });
	}

	@Override
	public void resetConnection() {
		updateZookeeperStatus(false);
		zk = null;
	}

	@Override
	public void run() {
		CountDownLatch latch = new CountDownLatch(1);
		while (true) {
			if (zk == null) {
				try {
					latch = new CountDownLatch(1);
					final CountDownLatch watcherLatch = latch;
					zk = new ZooKeeper(url, 3000, new Watcher() {

						@Override
						public void process(WatchedEvent event) {
							KeeperState state = event.getState();
							if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
								log.warn("zookeeper disconnected event");
								closeZookeeper();
							} else if (state == KeeperState.SyncConnected) {
								watcherLatch.countDown();
								log.info("zk session established");
							}
						}
					});
				} catch (IOException e) {
					closeZookeeper();
				}
			}

			if (zk == null) {
				log.info("Instance is not online.  Going to sleep for 30 seconds and try again.");
				updateZookeeperStatus(false);
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e) {
					log.info("Exiting Zookeeper instance");
					return;
				}
			} else {
				try {
					if (latch.await(10, TimeUnit.SECONDS)) {
						updateZookeeperStatus(true);
					} else {
						closeZookeeper();
						updateZookeeperStatus(false);
					}
				} catch (InterruptedException e1) {
					closeZookeeper();
					updateZookeeperStatus(false);
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

	private void closeZookeeper() {
		if (zk != null) {
			try {
				zk.close();
			} catch (InterruptedException e1) {
			}
		}
		zk = null;
	}

	private void handleZkError(Exception e) {
		closeZookeeper();
		if (!(e instanceof SessionExpiredException || e instanceof SessionMovedException)) {
			log.warn("error talking to zookeeper", e);
		} else {
			log.warn("zookeeper session expired");
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

	@Override
	public JdbcTemplate getJdbc() {
		return jdbc;
	}

	public String getName() {
		return name;
	}

}
