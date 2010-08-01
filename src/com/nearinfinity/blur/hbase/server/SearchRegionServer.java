package com.nearinfinity.blur.hbase.server;

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
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.zookeeper.HQuorumPeer;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.hbase.BlurHits;
import com.nearinfinity.blur.hbase.SearchRPC;
import com.nearinfinity.blur.hbase.ipc.SearchRegionInterface;
import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;

public class SearchRegionServer extends HRegionServer implements SearchRegionInterface {

	private static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";
	private static final String SERVER = "server.";
	private static final String CLIENT_PORT = "clientPort";
	private static final Log LOG = LogFactory.getLog(SearchRegionServer.class);
	
	static {
		SearchRPC.initialize();
	}
	
	private String quorumServers;
	private ZooKeeper zk;
	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private SearchExecutorImpl searchExecutor;

	public SearchRegionServer(HBaseConfiguration conf) throws IOException {
		super(conf);
		Properties properties = HQuorumPeer.makeZKProps(conf);
		setQuorumServers(properties);
		if (quorumServers == null) {
			throw new IOException("Could not read quorum servers from " + ZOOKEEPER_CONFIG_NAME);
		}
		int sessionTimeout = conf.getInt(ZOOKEEPER_SESSION_TIMEOUT, 60 * 1000);
		this.zk = new ZooKeeper(quorumServers, sessionTimeout, this);
		
		this.directoryManager = new DirectoryManagerImpl(zk);
		this.indexManager = new IndexManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		this.searchExecutor = new SearchExecutorImpl(searchManager);
	}

	@Override
	public BlurHits search(String query, String filter, long start, int fetchCount) {
		return searchExecutor.search(query, filter, start, fetchCount);
	}

	@Override
	public long searchFast(String query, String filter, long minimum) {
		return searchExecutor.searchFast(query, filter, minimum);
	}
	
	@Override
	public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
		if (protocol.equals(SearchRegionInterface.class.getName())) {
			return HBaseRPCProtocolVersion.versionID;
		}
		return super.getProtocolVersion(protocol, clientVersion);
	}
	
	private void setQuorumServers(Properties properties) {
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
