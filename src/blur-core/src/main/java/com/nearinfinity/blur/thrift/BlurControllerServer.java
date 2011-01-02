package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurUtil.getParametersList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableBlurClient;
import com.nearinfinity.blur.manager.hits.MergerFacetResult;
import com.nearinfinity.blur.manager.hits.MergerHitsIterable;
import com.nearinfinity.blur.manager.status.MergerSearchQueryStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.commands.BlurSearchCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FacetQuery;
import com.nearinfinity.blur.thrift.generated.FacetResult;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.LoggingBlurException;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurControllerServer extends BlurAdminServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
	
	private ExecutorService executor = Executors.newCachedThreadPool();
	private AtomicReference<Map<String,Map<String,String>>> shardServerLayout = new AtomicReference<Map<String,Map<String,String>>>(new HashMap<String, Map<String,String>>());
	private BlurClient client;
	private long delay = TimeUnit.SECONDS.toMillis(5);

    private Timer shardLayoutTimer;
    
    public BlurControllerServer() {
        shardLayoutTimer = new Timer("Shard-Layout-Timer", true);
        shardLayoutTimer.scheduleAtFixedRate(new TimerTask(){
            @Override
            public void run() {
                updateShardLayout();
            }
        }, delay, delay);
    }
    
    public void close() {
        shardLayoutTimer.cancel();
    }

    @Override
	public Hits search(final String table, final SearchQuery searchQuery) throws BlurException, TException {
		try {
		    HitsIterable hitsIterable = scatterGather(new BlurSearchCommand<HitsIterable>() {
                @Override
                public HitsIterable call(Client client) throws Exception {
                    return new HitsIterableBlurClient(client,table,searchQuery);
                }
            },new MergerHitsIterable(searchQuery.minimumNumberOfHits,searchQuery.maxQueryTime));
			return convertToHits(hitsIterable, searchQuery.start, searchQuery.fetch, searchQuery.minimumNumberOfHits);
		} catch (Exception e) {
			throw new LoggingBlurException(LOG,e,"Unknown error during search of [" +
                    getParametersList("table",table, "searchquery", searchQuery) + "]");
		}
	}
	
    @Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.CONTROLLER;
	}

	@Override
	public FetchResult fetchRow(final String table, final Selector selector) throws BlurException,
			TException {
	    String clientHostnamePort = getNode(table,selector);
		try {
		    return client.execute(clientHostnamePort, 
		        new BlurSearchCommand<FetchResult>() {
                    @Override
                    public FetchResult call(Client client) throws Exception {
                        return client.fetchRow(table, selector);
                    }
                });
		} catch (Exception e) {
		    throw new LoggingBlurException(LOG,e,"Unknown error during fetch of row from table [" + table +
                    "] selector [" + selector + "]");
        }
	}
	
    @Override
    public void cancelSearch(final long uuid) throws BlurException, TException {
        try {
            scatter(new BlurSearchCommand<Void>() {
                @Override
                public Void call(Client client) throws Exception {
                    client.cancelSearch(uuid);
                    return null;
                }
            });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to cancel search [" + uuid + "]");
        }
    }
    
    @Override
    public List<SearchQueryStatus> currentSearches(final String table) throws BlurException, TException {
        try {
            return scatterGather(new BlurSearchCommand<List<SearchQueryStatus>>() {
                @Override
                public List<SearchQueryStatus> call(Client client) throws Exception {
                    return client.currentSearches(table);
                }
            },new MergerSearchQueryStatus());
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get current searches [" + table + "]");
        }
    }
	   
    @Override
    public byte[] fetchRowBinary(final String table, final Selector selector) throws BlurException, TException {
        String clientHostnamePort = getNode(table,selector);
        try {
            return client.execute(clientHostnamePort, 
                new BlurSearchCommand<byte[]>() {
                    @Override
                    public byte[] call(Client client) throws Exception {
                        return client.fetchRowBinary(table, selector);
                    }
                });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error during fetch of row from table [" + table +
                    "] selector [" + selector + "]");
        }
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        return shardServerLayout.get().get(table);
    }
    
    public FacetResult facetSearch(final String table, final FacetQuery facetQuery) throws BlurException, TException {
        try {
            return scatterGather(
                    new BlurSearchCommand<FacetResult>() {
                        @Override
                        public FacetResult call(Client client) throws Exception {
                            return client.facetSearch(table,facetQuery);
                        }
                    },new MergerFacetResult());
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error during facet search [" + table + "/" + facetQuery + "]");
        }
    }

    @Override
    public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value) throws BlurException, TException {
        try {
            return scatterGather(
                new BlurSearchCommand<Long>() {
                    @Override
                    public Long call(Client client) throws Exception {
                        return client.recordFrequency(table,columnFamily,columnName,value);
                    }
                },new Merger<Long>() {
                    @Override
                    public Long merge(BlurExecutorCompletionService<Long> service) throws Exception {
                        Long total = 0L;
                        while (service.getRemainingCount() > 0) {
                            total += service.take().get();
                        }
                        return total;
                    }
                });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get record frequency [" + table + "/" + columnFamily + "/" + columnName + "/" + value + "]");
        }
    }

    @Override
    public Schema schema(String table) throws BlurException, TException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size) throws BlurException, TException {
        throw new RuntimeException("not implemented");
    }

    public BlurClient getClient() {
        return client;
    }

    public void setClient(BlurClient client) {
        this.client = client;
    }
    
    private String getNode(String table, Selector selector) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
    
    private void updateShardLayout() {
        try {
            Map<String,Map<String, String>> layout = new HashMap<String, Map<String,String>>();
            for (String t : tableList()) {
                final String table = t;
                layout.put(table, ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,Map<String,String>>() {
                    @Override
                    public Map<String,String> call(String hostnamePort) throws Exception {
                        return client.execute(hostnamePort, new BlurSearchCommand<Map<String,String>>() {
                            @Override
                            public Map<String,String> call(Client client) throws Exception {
                                return client.shardServerLayout(table);
                            }
                        });
                    }
                }).merge(new Merger<Map<String,String>>() {
                    @Override
                    public Map<String, String> merge(BlurExecutorCompletionService<Map<String, String>> service)
                            throws Exception {
                        Map<String,String> result = new HashMap<String, String>();
                        while (service.getRemainingCount() > 0) {
                            result.putAll(service.take().get());
                        }
                        return result;
                    }
                }));
            }
            shardServerLayout.set(layout);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to update shard layout.",e);
        }
    }
    
    private <R> R scatterGather(final BlurSearchCommand<R> command, Merger<R> merger) throws Exception {
        return ForkJoin.execute(executor, shardServerList(), new ParallelCall<String, R>() {
            @SuppressWarnings("unchecked")
            @Override
            public R call(String hostnamePort) throws Exception {
                return client.execute(hostnamePort, (BlurSearchCommand<R>) command.clone());
            }
        }).merge(merger);
    }
    
    private <R> void scatter(BlurSearchCommand<R> command) throws Exception {
        scatterGather(command,new Merger<R>() {
            @Override
            public R merge(BlurExecutorCompletionService<R> service) throws Exception {
                while (service.getRemainingCount() > 0) {
                    service.take().get();
                }
                return null;
            }
        });
    }

}