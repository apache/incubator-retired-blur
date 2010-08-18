package com.nearinfinity.blur.thrift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.nearinfinity.blur.zookeeper.ZookeeperDirectoryManagerStore;

public abstract class BlurAdminServer implements Iface,BlurConstants {
	
	private static final String DYNAMIC_TERMS = "dynamicTerms";
	private static final Logger LOG = LoggerFactory.getLogger(BlurAdminServer.class);
	private static final String NODES = "nodes";
	private static final String BLUR_REFS = "refs";
	
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
			if (hits == null) {
				return null;
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
	protected String blurPath;
	protected ZookeeperDirectoryManagerStore zookeeperDirectoryManagerStore = new ZookeeperDirectoryManagerStore();
	
	public BlurAdminServer() throws IOException {
		zk = ZooKeeperFactory.getZooKeeper();
		blurPath = configuration.get(BLUR_ZOOKEEPER_PATH);
		blurNodePath = configuration.get(BLUR_ZOOKEEPER_PATH) + "/" + NODES;
		try {
			registerNode();
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void create(String table, TableDescriptor desc) throws BlurException, TException {
		if (tableList().contains(table)) {
			throw new BlurException("table " + table + " already exists");
		}
		desc.isEnabled = false;
		try {
			save(table,desc);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}


	@Override
	public TableDescriptor describe(String table) throws BlurException, TException {
		try {
			return get(table);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}

	@Override
	public void disable(String table) throws BlurException, TException {
		TableDescriptor descriptor = describe(table);
		checkIfTableExists(descriptor,table);
		descriptor.isEnabled = false;
		try {
			save(table,descriptor);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
		removeAllTableShards(table);
	}

	@Override
	public void drop(String table) throws BlurException, TException {
		TableDescriptor descriptor = describe(table);
		checkIfTableExists(descriptor,table);
		if (descriptor.isEnabled) {
			throw new BlurException("table " + table + " must be disabled before drop");
		}
		try {
			remove(table);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}

	@Override
	public void enable(String table) throws BlurException, TException {
		TableDescriptor descriptor = describe(table);
		checkIfTableExists(descriptor,table);
		descriptor.isEnabled = true;
		try {
			save(table,descriptor);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
		try {
			createAllTableShards(table,descriptor);
		} catch (URISyntaxException e) {
			throw new BlurException(e.getMessage());
		}
	}

	@Override
	public void createDynamicTermQuery(String table, String term, String query, boolean superQueryOn)
		throws BlurException, TException {
		try {
			ZkUtils.mkNodes(ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table), zk);
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table,term);
			Stat stat = zk.exists(path, false);
			if (stat != null) {
				throw new BlurException("Dynamic term [" + term +
						"] already exists for table [" + table +
						"]");
			}
			byte[] bs = query.getBytes();
			byte b = 0;
			if (superQueryOn) {
				b = 1;
			}
			ByteBuffer buffer = ByteBuffer.allocate(bs.length + 1);
			zk.create(path, buffer.put(b).put(bs).array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteDynamicTermQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodes(ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table), zk);
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table,term);
			Stat stat = zk.exists(path, false);
			if (stat == null) {
				throw new BlurException("Dynamic term [" + term +
						"] does not exist for table [" + table +
						"]");
			}
			zk.delete(path, stat.getVersion());
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getDynamicTermQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodes(ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table), zk);
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table,term);
			Stat stat = zk.exists(path, false);
			if (stat == null) {
				throw new BlurException("Dynamic term [" + term +
						"] does not exist for table [" + table +
						"]");
			}
			ByteBuffer buffer = ByteBuffer.wrap(zk.getData(path, false, stat));
			return new String(buffer.array(),1,buffer.remaining());
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean isDynamicTermQuerySuperQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodes(ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table), zk);
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table,term);
			Stat stat = zk.exists(path, false);
			if (stat == null) {
				throw new BlurException("Dynamic term [" + term +
						"] does not exist for table [" + table +
						"]");
			}
			ByteBuffer buffer = ByteBuffer.wrap(zk.getData(path, false, stat));
			byte b = buffer.get();
			if (b == 0) {
				return false;
			} else {
				return true;
			}
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> getDynamicTerms(String table) throws BlurException, TException {
		try {
			ZkUtils.mkNodes(ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table), zk);
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table);
			return zk.getChildren(path, false);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> tableList() throws BlurException, TException {
		try {
			String path = ZkUtils.getPath(blurPath,BLUR_TABLES_NODE);
			List<String> children = zk.getChildren(path, false);
			return new ArrayList<String>(children);
		} catch (KeeperException e) {
			if (e.code().equals(Code.NONODE)) {
				return new ArrayList<String>();
			}
			throw new BlurException(e.getMessage());
		} catch (InterruptedException e) {
			throw new BlurException(e.getMessage());
		}
	}
	
	protected abstract NODE_TYPE getType();
	
	protected void registerNode() throws KeeperException, InterruptedException, IOException {
		InetAddress address = getMyAddress();
		String hostName = address.getHostAddress();
		NODE_TYPE type = getType();
		ZkUtils.mkNodes(blurNodePath, zk);
		ZkUtils.mkNodes(blurNodePath + "/" + type.name(), zk);
		while (true) {
			int retry = 10;
			try {
				zk.create(blurNodePath + "/" + type.name() + "/" + hostName, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				return;
			} catch (KeeperException e) {
				if (e.code().equals(Code.NODEEXISTS)) {
					if (retry > 0) {
						LOG.info("Waiting to register node {} as type {}, probably because node was shutdown and restarted...",hostName,type.name());
						Thread.sleep(1000);
						retry--;
						continue;
					}
				}
				throw e;
			}
		}
	}
	
	private InetAddress getMyAddress() throws UnknownHostException {
		return InetAddress.getLocalHost();
	}
	
	private TableDescriptor get(String table) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		ZkUtils.mkNodes(ZkUtils.getPath(blurPath,BLUR_TABLES_NODE), zk);
		String path = ZkUtils.getPath(blurPath,BLUR_TABLES_NODE,table);
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			return null;
		} else {
			byte[] data = zk.getData(path, false, stat);
			return readTableDescriptor(data);
		}
	}

	private void save(String table, TableDescriptor descriptor) throws KeeperException, InterruptedException, IOException {
		ZkUtils.mkNodes(ZkUtils.getPath(blurPath,BLUR_TABLES_NODE), zk);
		String path = ZkUtils.getPath(blurPath,BLUR_TABLES_NODE,table);
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			zk.create(path, writeTableDescriptor(descriptor), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			zk.setData(path, writeTableDescriptor(descriptor), stat.getVersion());
		}
	}
	
	private void remove(String table) throws InterruptedException, KeeperException, IOException {
		ZkUtils.mkNodes(ZkUtils.getPath(blurPath,BLUR_TABLES_NODE), zk);
		String path = ZkUtils.getPath(blurPath,BLUR_TABLES_NODE,table);
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			return;
		} else {
			zk.delete(path, stat.getVersion());
		}
	}
	
	private byte[] writeTableDescriptor(TableDescriptor descriptor) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream outputStream = new ObjectOutputStream(baos);
		String analyzerDef = descriptor.getAnalyzerDef();
		String partitionerClass = descriptor.getPartitionerClass();
		Map<String, String> shardDirectoryLocations = descriptor.getShardDirectoryLocations();
		outputStream.writeObject(analyzerDef);
		outputStream.writeObject(partitionerClass);
		outputStream.writeObject(shardDirectoryLocations);
		outputStream.close();
		return baos.toByteArray();
	}
	
	@SuppressWarnings("unchecked")
	private TableDescriptor readTableDescriptor(byte[] data) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(data));
		try {
			TableDescriptor descriptor = new TableDescriptor();
			descriptor.analyzerDef = (String) inputStream.readObject();
			descriptor.partitionerClass = (String) inputStream.readObject();
			descriptor.shardDirectoryLocations = (Map<String, String>) inputStream.readObject();
			return descriptor;
		} finally {
			inputStream.close();
		}
	}
	
	private void createAllTableShards(String table, TableDescriptor descriptor) throws URISyntaxException {
		Map<String, String> directoryLocations = descriptor.shardDirectoryLocations;
		for (String shardId : directoryLocations.keySet()) {
			URI dirUri = new URI(wrapWithZookeeperUri(table,shardId,directoryLocations.get(shardId)));
			zookeeperDirectoryManagerStore.addDirectoryURIToServe(table, shardId, dirUri);
		}
	}

	private String wrapWithZookeeperUri(String table, String shardId, String baseUri) {
		return "zk://" + blurPath +
				"/" + BLUR_REFS +
				"/" + table + 
				"/" + shardId + 
				"?" + baseUri;
	}

	private void removeAllTableShards(String table) {
		Set<String> shardIds = zookeeperDirectoryManagerStore.getShardIds(table);
		for (String shardId : shardIds) {
			zookeeperDirectoryManagerStore.removeDirectoryURIToServe(table, shardId);
		}
	}
	
	private void checkIfTableExists(TableDescriptor descriptor, String table) throws BlurException {
		if (descriptor == null) {
			throw new BlurException("Table " + table + " does not exist");
		}		
	}
}
