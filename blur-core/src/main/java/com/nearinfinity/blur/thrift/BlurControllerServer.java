package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurUtil.getParametersList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
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
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.LoggingBlurException;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurControllerServer extends BlurBaseServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
	
	private ExecutorService executor = Executors.newCachedThreadPool();
	private AtomicReference<Map<String,Map<String,String>>> shardServerLayout = new AtomicReference<Map<String,Map<String,String>>>(new HashMap<String, Map<String,String>>());
	private BlurClient client;
	private long delay = TimeUnit.SECONDS.toMillis(5);
	private Random random = new Random();
    private Timer shardLayoutTimer;
    private IndexServer indexServer;
    
    public void open() {
        updateShardLayout();
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
        executor.shutdownNow();
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
	public FetchResult fetchRow(final String table, final Selector selector) throws BlurException,
			TException {
	    String clientHostnamePort = null;
		try {
		    clientHostnamePort = getNode(table,selector);
		    return client.execute(clientHostnamePort, 
		        new BlurSearchCommand<FetchResult>() {
                    @Override
                    public FetchResult call(Client client) throws Exception {
                        return client.fetchRow(table, selector);
                    }
                });
		} catch (Exception e) {
		    throw new LoggingBlurException(LOG,e,"Unknown error during fetch of row from table [" + table +
                    "] selector [" + selector + "] node [" + clientHostnamePort + "]");
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
    public Schema schema(final String table) throws BlurException, TException {
        try {
            return scatterGather(
                new BlurSearchCommand<Schema>() {
                    @Override
                    public Schema call(Client client) throws Exception {
                        return client.schema(table);
                    }
                },new Merger<Schema>() {
                    @Override
                    public Schema merge(BlurExecutorCompletionService<Schema> service) throws Exception {
                        Schema result = null;
                        while (service.getRemainingCount() > 0) {
                            Schema schema = service.take().get();
                            if (result == null) {
                                result = schema;
                            } else {
                                result = BlurControllerServer.merge(result,schema);
                            }
                        }
                        return result;
                    }
                });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to schema table [" + table + "]");
        }
    }

    @Override
    public List<String> terms(final String table, final String columnFamily, final String columnName, final String startWith, final short size) throws BlurException, TException {
        try {
            return scatterGather(
                new BlurSearchCommand<List<String>>() {
                    @Override
                    public List<String> call(Client client) throws Exception {
                        return client.terms(table,columnFamily,columnName,startWith,size);
                    }
                },new Merger<List<String>>() {
                    @Override
                    public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws Exception {
                        TreeSet<String> terms = new TreeSet<String>();
                        while (service.getRemainingCount() > 0) {
                            terms.addAll(service.take().get());
                        }
                        return new ArrayList<String>(terms).subList(0, size);
                    }
                });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to terms table [" + table +
            		"] columnFamily [" + columnFamily + 
            		"] columnName [" + columnName + 
            		"] startWith [" + startWith + 
            		"] size [" + size + 
            		"]");
        }
    }

    public BlurClient getClient() {
        return client;
    }

    public void setClient(BlurClient client) {
        this.client = client;
    }
    
    private String getNode(String table, Selector selector) throws BlurException, TException {
        Map<String, String> layout = shardServerLayout(table);
        String locationId = selector.locationId;
        String shard = locationId.substring(0, locationId.indexOf('/'));
        return layout.get(shard);
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

    @Override
    public IndexServer getIndexServer() {
        return indexServer;
    }
    
    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }

    @Override
    public TableDescriptor describe(final String table) throws BlurException, TException {
        try {
            String hostname = getSingleOnlineShardServer();
            return client.execute(hostname, new BlurSearchCommand<TableDescriptor>() {
                @Override
                public TableDescriptor call(Client client) throws Exception {
                    return client.describe(table);
                }
            });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to describe table [" + table + "]");
        }
    }
    
    @Override
    public List<String> tableList() throws BlurException, TException {
        try {
            String hostname = getSingleOnlineShardServer();
            return client.execute(hostname, new BlurSearchCommand<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.tableList();
                }
            });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get table list.  Current online shard servers [" +
            		indexServer.getOnlineShardServers() + "]");
        }
    }
    
    private String getSingleOnlineShardServer() throws BlurException, TException {
        List<String> onlineShardServers = indexServer.getOnlineShardServers();
        return onlineShardServers.get(random.nextInt(onlineShardServers.size()));
    }

    public static Schema merge(Schema result, Schema schema) {
        Map<String, Set<String>> destColumnFamilies = result.columnFamilies;
        Map<String, Set<String>> srcColumnFamilies = schema.columnFamilies;
        for (String srcColumnFamily : srcColumnFamilies.keySet()) {
            Set<String> destColumnNames = destColumnFamilies.get(srcColumnFamily);
            Set<String> srcColumnNames = srcColumnFamilies.get(srcColumnFamily);
            if (destColumnNames == null) {
                destColumnFamilies.put(srcColumnFamily, srcColumnNames);
            } else {
                destColumnNames.addAll(srcColumnNames);
            }
        }
        return result;
    }
}