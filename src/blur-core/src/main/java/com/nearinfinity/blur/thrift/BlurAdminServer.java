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
import com.nearinfinity.mele.util.AddressUtil;

public abstract class BlurAdminServer implements Iface, BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurAdminServer.class);
	
	public enum NODE_TYPE {
		CONTROLLER,
		SHARD
	}
	
	protected BlurConfiguration configuration;
	protected MetaData metaData;
	protected EventHandler handler = new EmptyEventHandler();
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
	public void createDynamicTermQuery(String table, String term, String query, boolean superQueryOn)
		throws BlurException, TException {
	    metaData.createDynamicTermQuery(table, term, query, superQueryOn);
	}

	@Override
	public void deleteDynamicTermQuery(String table, String term) throws BlurException, TException {
	    metaData.deleteDynamicTermQuery(table, term);
	}

	@Override
	public String getDynamicTermQuery(String table, String term) throws BlurException, TException {
	    return metaData.getDynamicTermQuery(table, term);
	}
	
	@Override
	public boolean isDynamicTermQuerySuperQuery(String table, String term) throws BlurException, TException {
	    return metaData.isDynamicTermQuerySuperQuery(table,term);
	}

	@Override
	public List<String> getDynamicTerms(String table) throws BlurException, TException {
	    return metaData.getDynamicTerms(table);
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
    
    private static List<String> addPort(List<String> hosts, int port) {
        List<String> result = new ArrayList<String>();
        for (String host : hosts) {
            result.add(host + ":" + port);
        }
        return result;
    }
}
