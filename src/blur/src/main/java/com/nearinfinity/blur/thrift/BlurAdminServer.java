package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ZkUtils;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.zookeeper.ZooKeeperFactory;

public abstract class BlurAdminServer implements Iface,BlurConstants {
	
	private static final String NODES = "nodes";
	
	public static class HitsMerger implements Merger<Hits> {
		@Override
		public Hits merge(List<Future<Hits>> futures) throws Exception {
			Hits hits = null;
			for (Future<Hits> future : futures) {
				if (hits == null) {
					hits = future.get();
				} else {
					hits = mergeHits(hits,future.get());
				}
			}
			sortHits(hits.hits);
			return hits;
		}
		
		private void sortHits(List<Hit> hits) {
			if (hits == null) {
				return;
			}
			Collections.sort(hits, new Comparator<Hit>() {
				@Override
				public int compare(Hit o1, Hit o2) {
					if (o1.score == o2.score) {
						return o1.id.compareTo(o2.id);
					}
					return Double.compare(o1.score, o2.score);
				}
			});
		}

		protected Hits mergeHits(Hits existing, Hits newHits) {
			existing.totalHits += newHits.totalHits;
			if (existing.shardInfo == null) {
				existing.shardInfo = newHits.shardInfo;
			} else {
				existing.shardInfo.putAll(newHits.shardInfo);
			}
			if (existing.hits == null) {
				existing.hits = newHits.hits;
			} else {
				existing.hits.addAll(newHits.hits);
			}
			return existing;		
		}
	}
	
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
