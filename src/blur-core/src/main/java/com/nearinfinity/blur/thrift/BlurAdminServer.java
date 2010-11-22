package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.metadata.MetaData;
import com.nearinfinity.blur.thrift.commands.BlurAdminCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Client;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Iface;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.util.AddressUtil;

public abstract class BlurAdminServer implements Iface, BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurAdminServer.class);
	
	public enum NODE_TYPE {
		CONTROLLER,
		SHARD
	}
	
	protected BlurConfiguration configuration;
	protected MetaData metaData;
	protected ExecutorService executor = Executors.newCachedThreadPool();
	
	public BlurAdminServer(MetaData metaData, BlurConfiguration configuration) throws IOException {
	    this.configuration = configuration;
		this.metaData = metaData;
		metaData.registerNode(AddressUtil.getMyHostName(), getType());
	}
	
	@Override
    public List<String> controllerServerList() throws BlurException, TException {
        return addPort(metaData.getControllerServerHosts(),configuration.getBlurControllerServerPort());
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return addPort(metaData.getShardServerHosts(),configuration.getBlurShardServerPort());
    }

    @Override
	public void create(String table, TableDescriptor desc) throws BlurException, TException {
		metaData.create(table,desc);
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        return metaData.shardServerLayout(table);
    }

    @Override
	public TableDescriptor describe(String table) throws BlurException, TException {
		return metaData.describe(table);
	}

	@Override
	public void disable(String table) throws BlurException, TException {
		metaData.disable(table);
	}

	@Override
	public void drop(String table) throws BlurException, TException {
		metaData.drop(table);
	}

	@Override
	public void enable(String table) throws BlurException, TException {
		metaData.enable(table);
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
	public List<String> tableList() throws BlurException, TException {
	    return metaData.tableList();
	}
	
	protected abstract NODE_TYPE getType();
	
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
        if (hits.hits == null) {
            hits.hits = new ArrayList<Hit>();
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
            BlurClientManager.execute(node + ":" + configuration.getBlurControllerServerPort(), new BlurAdminCommand<Boolean>() {
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
            BlurClientManager.execute(node + ":" + configuration.getBlurControllerServerPort(), new BlurAdminCommand<Boolean>() {
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
    public void cancelSearch(long providedUuid) throws BlurException, TException, EventStoppedExecutionException {
        throw new BlurException("not implemented");
    }

    @Override
    public FetchResult fetchRow(String table, Selector selector) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        throw new BlurException("not implemented");
    }
    
    @Override
    public void removeRow(String table, String id) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        throw new BlurException("not implemented");
    }

    @Override
    public void replaceRow(String table, Row row) throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
        throw new BlurException("not implemented");
    }

    private boolean isThisNode(String node) throws UnknownHostException {
        if (AddressUtil.getMyHostName().equals(node)) {
            return true;
        }
        return false;
    }
    
    private static List<String> addPort(List<String> hosts, int port) {
        List<String> result = new ArrayList<String>();
        for (String host : hosts) {
            result.add(host + ":" + port);
        }
        return result;
    }
}
