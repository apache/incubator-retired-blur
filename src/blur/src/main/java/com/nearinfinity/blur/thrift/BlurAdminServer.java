package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ZkUtils;
import com.nearinfinity.blur.zookeeper.ZooKeeperFactory;

public abstract class BlurAdminServer implements Iface,BlurConstants {
	
	private static final String NODES = "nodes";
	
	public enum NODE_TYPE {
		CONTROLLER,
		NODE
	}

	public enum REQUEST_TYPE {
		STATUS,
		SEARCH,
		FAST_SEARCH,
		UNKNOWN
	}
	
	protected ExecutorService executor = Executors.newCachedThreadPool();
	protected ZooKeeper zk;
	protected String blurNodePath;
	protected BlurConfiguration configuration = new BlurConfiguration();
	
	public BlurAdminServer() throws IOException {
		zk = ZooKeeperFactory.getZooKeeper();
		blurNodePath = configuration.get(BLUR_ZOOKEEPER_PATH) + "/" + NODES;
		registerNode();
	}

	@Override
	public void create(String table, TableDescriptor desc) throws BlurException, TException {

	}

	@Override
	public void createDynamicTermQuery(String table, String term, String query, boolean superQueryOn)
			throws BlurException, TException {

	}

	@Override
	public void deleteDynamicTermQuery(String table, String term) throws BlurException, TException {

	}

	@Override
	public TableDescriptor describe(String table) throws BlurException, TException {
		return null;
	}

	@Override
	public void disable(String table) throws BlurException, TException {

	}

	@Override
	public void drop(String table) throws BlurException, TException {

	}

	@Override
	public void enable(String table) throws BlurException, TException {

	}

	@Override
	public String getDynamicTermQuery(String table, String term) throws BlurException, TException {
		return null;
	}

	@Override
	public List<String> getDynamicTerms(String table) throws BlurException, TException {
		return null;
	}

	@Override
	public List<String> tableList() throws BlurException, TException {
		return null;
	}
	
	protected abstract NODE_TYPE getType();
	
	protected void registerNode() {
		try {
			InetAddress address = getMyAddress();
			String hostName = address.getHostAddress();
			NODE_TYPE type = getType();
			ZkUtils.mkNodes(blurNodePath, zk);
			ZkUtils.mkNodes(blurNodePath + "/" + type.name(), zk);
			zk.create(blurNodePath + "/" + type.name() + "/" + hostName, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private InetAddress getMyAddress() throws UnknownHostException {
		return InetAddress.getLocalHost();
	}
}
