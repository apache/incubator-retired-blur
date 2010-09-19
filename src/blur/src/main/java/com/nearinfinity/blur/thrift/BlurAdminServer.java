package com.nearinfinity.blur.thrift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.BlurClientManager.Command;
import com.nearinfinity.blur.thrift.events.EmptyEventHandler;
import com.nearinfinity.blur.thrift.events.EventHandler;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.store.util.AddressUtil;
import com.nearinfinity.mele.store.util.ZkUtils;

public abstract class BlurAdminServer implements Iface, BlurConstants, Watcher {
	
	private static final Log LOG = LogFactory.getLog(BlurAdminServer.class);
	private static final String DYNAMIC_TERMS = "dynamicTerms";
	private static final String NODES = "nodes";
	
	public enum NODE_TYPE {
		CONTROLLER,
		SHARD
	}

	protected ExecutorService executor = Executors.newCachedThreadPool();
	protected ZooKeeper zk;
	protected String blurNodePath;
	protected BlurConfiguration configuration;
	protected String blurPath;
	protected Mele mele;
	protected List<String> shardServerHosts = new ArrayList<String>();
	protected List<String> controllerServerHosts = new ArrayList<String>();
	protected EventHandler handler = new EmptyEventHandler();
	
	public BlurAdminServer(ZooKeeper zooKeeper, Mele mele, BlurConfiguration configuration) throws IOException {
	    this.configuration = configuration;
		this.zk = zooKeeper;
		this.blurPath = configuration.get(BLUR_ZOOKEEPER_PATH,BLUR_ZOOKEEPER_PATH_DEFAULT);
		this.blurNodePath = blurPath + "/" + NODES;
		try {
			registerNode();
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		process(null);
		this.mele = mele;
	}
	
	@Override
    public List<String> controllerServerList() throws BlurException, TException {
        return addPort(controllerServerHosts,configuration.getBlurControllerServerPort());
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return addPort(shardServerHosts,configuration.getBlurShardServerPort());
    }

    @Override
	public void create(String table, TableDescriptor desc) throws BlurException, TException {
		if (tableList().contains(table)) {
			throw new BlurException("Table " + table + " already exists.");
		}
		desc.isEnabled = false;
		try {
			save(table,desc);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        try {
            TableDescriptor descriptor = describe(table);
            Map<String,String> result = new TreeMap<String, String>();
            List<String> shardNames = descriptor.shardNames;
            for (String shardName : shardNames) {
                String lockPath = Mele.getLockPath(configuration, table, shardName);
                byte[] data = zk.getData(lockPath + "/" + "write.lock", false, null);
                result.put(shardName, new String(data));
            }
            return result;
        } catch (Exception e) {
            LOG.error("Unkown error while trying to create layout for table [" + table + "]",e);
            throw new BlurException("Unkown error while trying to create layout for table [" + table + "]");
        }
    }

    @Override
	public TableDescriptor describe(String table) throws BlurException, TException {
		try {
			TableDescriptor tableDescriptor = get(table);
			if (tableDescriptor == null) {
			    throw new BlurException("Table " + table + " does not exist.");
			}
            return tableDescriptor;
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}

	@Override
	public void disable(String table) throws BlurException, TException {
		TableDescriptor descriptor = describe(table);
		descriptor.isEnabled = false;
		try {
			save(table,descriptor);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
	}

	@Override
	public void drop(String table) throws BlurException, TException {
		TableDescriptor descriptor = describe(table);
		if (descriptor.isEnabled) {
			throw new BlurException("Table " + table + " must be disabled before dropping.");
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
		if (descriptor.isEnabled) {
		    return;
		}
		descriptor.isEnabled = true;
		try {
			save(table,descriptor);
		} catch (Exception e) {
			throw new BlurException(e.getMessage());
		}
		try {
			createAllTableShards(table,descriptor);
		} catch (IOException e) {
			throw new BlurException(e.getMessage());
		}
	}
	
	public boolean isTableEnabled(String table) {
		try {
			TableDescriptor describe = describe(table);
			if (describe == null) {
				return false;
			}
			return describe.isEnabled;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createDynamicTermQuery(String table, String term, String query, boolean superQueryOn)
		throws BlurException, TException {
		try {
			ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table));
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
		}
	}

	@Override
	public void deleteDynamicTermQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table));
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
		}
	}

	@Override
	public String getDynamicTermQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table));
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
		}
	}
	
	@Override
	public boolean isDynamicTermQuerySuperQuery(String table, String term) throws BlurException, TException {
		try {
			ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table));
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
		}
	}

	@Override
	public List<String> getDynamicTerms(String table) throws BlurException, TException {
		try {
			ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table));
			String path = ZkUtils.getPath(blurPath,DYNAMIC_TERMS,table);
			return zk.getChildren(path, false);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
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
	
	@Override
    public void process(WatchedEvent event) {
	    shardServerHosts = updateNodeLists(NODE_TYPE.SHARD);
	    controllerServerHosts = updateNodeLists(NODE_TYPE.CONTROLLER);
    }

    protected void registerNode() throws KeeperException, InterruptedException, IOException {
		String hostName = AddressUtil.getMyHostName();
		NODE_TYPE type = getType();
		ZkUtils.mkNodesStr(zk, blurNodePath);
		ZkUtils.mkNodesStr(zk, blurNodePath + "/" + type.name());
		while (true) {
			int retry = 10;
			try {
				zk.create(blurNodePath + "/" + type.name() + "/" + hostName, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				return;
			} catch (KeeperException e) {
				if (e.code().equals(Code.NODEEXISTS)) {
					if (retry > 0) {
						LOG.info("Waiting to register node [" + hostName +
								"] as type [" + type.name() +
								"], probably because node was shutdown and restarted...");
						Thread.sleep(1000);
						retry--;
						continue;
					}
				}
				throw e;
			}
		}
	}
	
	protected List<String> updateNodeLists(NODE_TYPE type) {
	     try {
          String path = blurNodePath + "/" + type.name();
          ZkUtils.mkNodesStr(zk, path);
          return new ArrayList<String>(zk.getChildren(path, this));
      } catch (Exception e) {
          throw new RuntimeException(e);
      }
	}
	
	private TableDescriptor get(String table) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,BLUR_TABLES_NODE));
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
		ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,BLUR_TABLES_NODE));
		String path = ZkUtils.getPath(blurPath,BLUR_TABLES_NODE,table);
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			zk.create(path, writeTableDescriptor(descriptor), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			zk.setData(path, writeTableDescriptor(descriptor), stat.getVersion());
		}
	}
	
	private void remove(String table) throws InterruptedException, KeeperException, IOException {
		ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath,BLUR_TABLES_NODE));
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
		List<String> shardNames = descriptor.getShardNames();
		outputStream.writeBoolean(descriptor.isEnabled);
		outputStream.writeObject(analyzerDef);
		outputStream.writeObject(partitionerClass);
		outputStream.writeObject(shardNames);
		outputStream.close();
		return baos.toByteArray();
	}
	
	@SuppressWarnings("unchecked")
	private TableDescriptor readTableDescriptor(byte[] data) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(data));
		try {
			TableDescriptor descriptor = new TableDescriptor();
			descriptor.isEnabled = inputStream.readBoolean();
			descriptor.analyzerDef = (String) inputStream.readObject();
			descriptor.partitionerClass = (String) inputStream.readObject();
			descriptor.shardNames = (List<String>) inputStream.readObject();
			return descriptor;
		} finally {
			inputStream.close();
		}
	}
	
	private void createAllTableShards(String table, TableDescriptor descriptor) throws IOException {
		mele.createDirectoryCluster(table);
		List<String> shardNames = descriptor.shardNames;
		for (String shard : shardNames) {
			mele.createDirectory(table, shard);
		}
	}
	
    private static List<String> addPort(List<String> hosts, int port) {
        List<String> result = new ArrayList<String>();
        for (String host : hosts) {
            result.add(host + ":" + port);
        }
        return result;
    }
    
    public static Hits convertToHits(HitsIterable hitsIterable, long start, int fetch, long minimumNumberOfHits) {
        Hits hits = new Hits();
        hits.setTotalHits(hitsIterable.getTotalHits());
        hits.setShardInfo(hitsIterable.getShardInfo());
        if (minimumNumberOfHits > 0) {
            hitsIterable.skipTo(start);
            int count = 0;
            Iterator<Hit> iterator = hitsIterable.iterator();
            while (iterator.hasNext() && count < fetch) {
                hits.addToHits(iterator.next());
                count++;
            }
        }
        return hits;
    }
    
    public static String getParametersList(Object... params) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < params.length; i+=2) {
            if (i != 0) {
                builder.append(',');
            }
            builder.append('[').append(params[i]).
                append(']').append('=').append('[').append(params[i+1]).append(']');
        }
        return builder.toString();
    }

    @Override
    public void shutdownController(final String node) throws BlurException, TException {
        try {
            if (isThisNode(node)) {
                System.exit(0);
            }
            BlurClientManager.execute(node + ":" + configuration.getBlurControllerServerPort(), new Command<Boolean>() {
                @Override
                public Boolean call(Client client) throws Exception {
                    client.shutdownController(node);
                    return true;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to shutdown controller [" + node + "]",e);
            throw new BlurException("Unknown error while trying to shutdown controller [" + node + "]");
        }
    }

    @Override
    public void shutdownShard(final String node) throws BlurException, TException {
        try {
            if (isThisNode(node)) {
                System.exit(0);
            }
            BlurClientManager.execute(node + ":" + configuration.getBlurControllerServerPort(), new Command<Boolean>() {
                @Override
                public Boolean call(Client client) throws Exception {
                    client.shutdownShard(node);
                    return true;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to shutdown controller [" + node + "]",e);
            throw new BlurException("Unknown error while trying to shutdown controller [" + node + "]");
        }
    }
    
    @Override
    public final void appendRow(String table, Row row) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        if (handler.beforeAppendRow(this, table, row)) {
            appendRowInternal(table, row);
            handler.afterAppendRow(this, table, row);
            return;
        }
        throw new EventStoppedExecutionException("Append Row Event Stopped.");
    }

    @Override
    public final void cancelSearch(long providedUuid) throws BlurException, TException, EventStoppedExecutionException {
        if (handler.beforeCancelSearch(this, providedUuid)) {
            cancelSearchInternal(providedUuid);
            handler.afterCancelSearch(this, providedUuid);
            return;
        }
        throw new EventStoppedExecutionException("Concel Search Event Stopped.");
    }

    @Override
    public final FetchResult fetchRow(String table, Selector selector) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        if (handler.beforeFetchRow(this, table, selector)) {
            FetchResult fetchResult = fetchRowInternal(table,selector);
            return handler.afterFetchRow(this, table, selector, fetchResult);
        }
        throw new EventStoppedExecutionException("FetchRow Event Stopped.");
    }
    
    @Override
    public final void removeRow(String table, String id) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        if (handler.beforeRemoveRow(this, table, id)) {
            removeRowInternal(table,id);
            handler.afterRemoveRow(this, table, id);
            return;
        }
        throw new EventStoppedExecutionException("Remove Row Event Stopped.");
    }

    @Override
    public final void replaceRow(String table, Row row) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        if (handler.beforeReplaceRow(this, table, row)) {
            replaceRowInternal(table,row);
            handler.afterReplaceRow(this, table, row);
            return;
        }
        throw new EventStoppedExecutionException("Replace Row Event Stopped.");
    }
    
    @Override
    public final Hits search(String table, SearchQuery searchQuery) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        if (handler.beforeSearch(this, table, searchQuery)) {
            Hits hits = searchInternal(table, searchQuery);
            return handler.afterSearch(this, table, searchQuery, hits);
        }
        throw new EventStoppedExecutionException("Search Event Stopped.");
    }

    public abstract void appendRowInternal(String table, Row row) throws BlurException, MissingShardException, TException;
    public abstract void cancelSearchInternal(long providedUuid) throws BlurException, TException;
    public abstract FetchResult fetchRowInternal(String table, Selector selector) throws BlurException, MissingShardException, TException;
    public abstract void removeRowInternal(String table, String id) throws BlurException, MissingShardException, TException;
    public abstract void replaceRowInternal(String table, Row row) throws BlurException, MissingShardException, TException;
    public abstract Hits searchInternal(String table, SearchQuery searchQuery) throws BlurException, MissingShardException, TException;
    
    
    private boolean isThisNode(String node) throws UnknownHostException {
        if (AddressUtil.getMyHostName().equals(node)) {
            return true;
        }
        return false;
    }
}
