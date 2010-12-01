package com.nearinfinity.blur.thrift;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.commands.BlurAdminCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
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
	
	private BlurConfiguration configuration;
	private IndexServer indexServer;
	
	@Override
    public List<String> controllerServerList() throws BlurException, TException {
        return indexServer.getControllerServerList();
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return indexServer.getShardServerList();
    }

    @Override
	public TableDescriptor describe(String table) throws BlurException, TException {
        Map<String, String> shardServerLayout = shardServerLayout(table);
        TableDescriptor descriptor = new TableDescriptor();
        descriptor.analyzerDef = indexServer.getAnalyzer(table).toString();
        descriptor.shardNames = new ArrayList<String>(shardServerLayout.keySet());
        descriptor.isEnabled = isTableEnabled(table);
		return descriptor;
	}

	public boolean isTableEnabled(String table) {
	    TABLE_STATUS tableStatus = indexServer.getTableStatus(table);
	    if (tableStatus == TABLE_STATUS.ENABLED) {
	        return true;
	    } else {
	        return false;
	    }
	}

	@Override
	public List<String> tableList() throws BlurException, TException {
	    return indexServer.getTableList();
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

    private boolean isThisNode(String node) throws UnknownHostException {
        if (AddressUtil.getMyHostName().equals(node)) {
            return true;
        }
        return false;
    }

    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }

    public BlurConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(BlurConfiguration configuration) {
        this.configuration = configuration;
    }
}
