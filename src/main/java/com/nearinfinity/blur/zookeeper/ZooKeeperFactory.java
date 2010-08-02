package com.nearinfinity.blur.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.HQuorumPeer;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperFactory implements HConstants {
	private static final Log LOG = LogFactory.getLog(ZooKeeperFactory.class);
	private static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";
	private static final String SERVER = "server.";
	private static final String CLIENT_PORT = "clientPort";
	private static String quorumServers;
	private static ZooKeeper zk;

	public static synchronized ZooKeeper getZooKeeper() throws IOException {
		if (zk == null) {
			init();
		}
		return zk;
	}
	
	private static void init() throws IOException {
		HBaseConfiguration conf = new HBaseConfiguration();
		Properties properties = HQuorumPeer.makeZKProps(conf);
		setQuorumServers(properties);
		if (quorumServers == null) {
			throw new IOException("Could not read quorum servers from " + ZOOKEEPER_CONFIG_NAME);
		}
		int sessionTimeout = conf.getInt(ZOOKEEPER_SESSION_TIMEOUT, 60 * 1000);
		zk = new ZooKeeper(quorumServers, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				//nothing
			}
		});		
	}

	private static void setQuorumServers(Properties properties) {
		String clientPort = null;
		List<String> servers = new ArrayList<String>();
		boolean anyValid = false;
		for (Entry<Object, Object> property : properties.entrySet()) {
			String key = property.getKey().toString().trim();
			String value = property.getValue().toString().trim();
			if (key.equals(CLIENT_PORT)) {
				clientPort = value;
			} else if (key.startsWith(SERVER)) {
				String host = value.substring(0, value.indexOf(':'));
				servers.add(host);
				try {
					InetAddress.getByName(host);
					anyValid = true;
				} catch (UnknownHostException e) {
					LOG.warn(StringUtils.stringifyException(e));
				}
			}
		}
		if (!anyValid) {
			LOG.error("no valid quorum servers found in " + ZOOKEEPER_CONFIG_NAME);
			return;
		}
		if (clientPort == null) {
			LOG.error("no clientPort found in " + ZOOKEEPER_CONFIG_NAME);
			return;
		}
		if (servers.isEmpty()) {
			LOG.fatal("No server.X lines found in conf/zoo.cfg. HBase must have a "
					+ "ZooKeeper cluster configured for its operation.");
			return;
		}
		StringBuilder hostPortBuilder = new StringBuilder();
		for (int i = 0; i < servers.size(); ++i) {
			String host = servers.get(i);
			if (i > 0) {
				hostPortBuilder.append(',');
			}
			hostPortBuilder.append(host);
			hostPortBuilder.append(':');
			hostPortBuilder.append(clientPort);
		}
		quorumServers = hostPortBuilder.toString();
	}

}
