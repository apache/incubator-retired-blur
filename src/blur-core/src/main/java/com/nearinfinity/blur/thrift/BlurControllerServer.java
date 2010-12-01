package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurUtil.getParametersList;

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
import com.nearinfinity.blur.manager.status.MergerSearchQueryStatus;
import com.nearinfinity.blur.thrift.commands.BlurAdminCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

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
    public void cancelSearch(final long uuid) throws BlurException, TException {
        try {
            ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,Void>() {
                @Override
                public Void call(String hostnamePort) throws Exception {
                    BlurClientManager.execute(hostnamePort, new BlurAdminCommand<Void>() {
                        @Override
                        public Void call(Client client) throws Exception {
                            client.cancelSearch(uuid);
                            return null;
                        }
                    });
                    return null;
                }
            }).merge(new Merger<Void>() {
                @Override
                public Void merge(BlurExecutorCompletionService<Void> service) throws Exception {
                    while (service.getRemainingCount() > 0) {
                        service.take().get();
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to cancel search [" + uuid + "]",e);
            throw new BlurException("Unknown error while trying to cancel search [" + uuid + "]");
        }
    }
    
    @Override
    public List<SearchQueryStatus> currentSearches(final String table) throws BlurException, TException {
        try {
            return ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,List<SearchQueryStatus>>() {
                @Override
                public List<SearchQueryStatus> call(String hostnamePort) throws Exception {
                    return BlurClientManager.execute(hostnamePort, new BlurAdminCommand<List<SearchQueryStatus>>() {
                        @Override
                        public List<SearchQueryStatus> call(Client client) throws Exception {
                            return client.currentSearches(table);
                        }
                    });
                }
            }).merge(new MergerSearchQueryStatus());
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get current searches [" + table + "]",e);
            throw new BlurException("Unknown error while trying to get current searches [" + table + "]");
        }
    }
	   
    @Override
    public byte[] fetchRowBinary(final String table, Selector selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new BlurException("not implemented");
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
    
    private String getClientHostnamePort(String table, Selector selector) throws MissingShardException, BlurException, TException {
        throw new BlurException("not implemented");
    }
}