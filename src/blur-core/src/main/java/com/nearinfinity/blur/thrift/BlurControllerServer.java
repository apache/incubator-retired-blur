package com.nearinfinity.blur.thrift;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableBlurClient;
import com.nearinfinity.blur.manager.hits.MergerHitsIterable;
import com.nearinfinity.blur.thrift.commands.BlurAdminCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import static com.nearinfinity.blur.utils.BlurUtil.*;

public class BlurControllerServer extends BlurAdminServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
	
	private ExecutorService executor = Executors.newCachedThreadPool();

	@Override
	public Hits search(final String table, final SearchQuery searchQuery) throws BlurException, TException {
		try {
			HitsIterable hitsIterable = ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,HitsIterable>() {
				@Override
				public HitsIterable call(final String hostnamePort) throws Exception {
					return BlurClientManager.execute(hostnamePort, new BlurAdminCommand<HitsIterable>() {
		                @Override
		                public HitsIterable call(Client client) throws Exception {
		                    return new HitsIterableBlurClient(client,hostnamePort,table,searchQuery);
		                }
		            });
				}
			}).merge(new MergerHitsIterable(searchQuery.minimumNumberOfHits,searchQuery.maxQueryTime));
			return convertToHits(hitsIterable, searchQuery.start, searchQuery.fetch, searchQuery.minimumNumberOfHits);
		} catch (Exception e) {
			LOG.error("Unknown error during search of [" +
					getParametersList("table",table, "searchquery", searchQuery) + "]",e);
			throw new BlurException(e.getMessage());
		}
	}
	
    @Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.CONTROLLER;
	}

	@Override
	public FetchResult fetchRow(final String table, final Selector selector) throws BlurException,
			TException, MissingShardException {
	    String clientHostnamePort = getClientHostnamePort(table,selector);
		try {
		    return BlurClientManager.execute(clientHostnamePort, 
		        new BlurAdminCommand<FetchResult>() {
                    @Override
                    public FetchResult call(Client client) throws Exception {
                        return client.fetchRow(table, selector);
                    }
                });
		} catch (Exception e) {
		    LOG.error("Unknown error during fetch of row from table [" + table +
		    		"] selector [" + selector + "]",e);
		    throw new BlurException("Unknown error during fetch of row from table [" + table +
                    "] selector [" + selector + "]");
        }
	}
	
    @Override
    public void cancelSearch(long userUuid) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
    
    @Override
    public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
	   
    private String getClientHostnamePort(String table, Selector selector) throws MissingShardException, BlurException, TException {
        throw new BlurException("not implemented");
    }
    
    @Override
    public byte[] fetchRowBinary(final String table, final String id, final byte[] selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new BlurException("not implemented");
    }

    @Override
    public void replaceRowBinary(final String table, final String id, final byte[] rowBytes) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new BlurException("not implemented");
    }

    @Override
    public void batchUpdate(String batchId, String table, Map<String, String> shardsToUris) throws BlurException,
            MissingShardException, TException {
        throw new BlurException("not implemented");
    }
    
    @Override
    public void removeRow(final String table, final String id) throws BlurException,
            TException, MissingShardException {
        throw new BlurException("not implemented");
    }

    @Override
    public void replaceRow(final String table, final Row row) throws BlurException,
            TException, MissingShardException {
        throw new BlurException("not implemented");
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
}