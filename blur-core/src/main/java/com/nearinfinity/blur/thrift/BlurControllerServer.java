/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.thrift;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableBlurClient;
import com.nearinfinity.blur.manager.hits.MergerHitsIterable;
import com.nearinfinity.blur.manager.indexserver.ClusterStatus;
import com.nearinfinity.blur.manager.status.MergerSearchQueryStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Client;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.LoggingBlurException;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurControllerServer implements Iface {
	
	private static final String CONTROLLER_THREAD_POOL = "controller-thread-pool";

    private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
	
	private ExecutorService executor;
	private AtomicReference<Map<String,Map<String,String>>> shardServerLayout = new AtomicReference<Map<String,Map<String,String>>>(new HashMap<String, Map<String,String>>());
	private BlurClient client;
	private long delay = TimeUnit.SECONDS.toMillis(5);
	private Random random = new Random();
    private Timer shardLayoutTimer;
    private ClusterStatus clusterStatus;
    private int threadCount = 64;
    private boolean closed;
    
    public void open() {
        executor = Executors.newThreadPool(CONTROLLER_THREAD_POOL,threadCount);
        updateShardLayout();
        shardLayoutTimer = new Timer("Shard-Layout-Timer", true);
        shardLayoutTimer.scheduleAtFixedRate(new TimerTask(){
            @Override
            public void run() {
                updateShardLayout();
            }
        }, delay, delay);
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            shardLayoutTimer.cancel();
            executor.shutdownNow();
        }
    }

    @Override
	public Hits search(final String table, final SearchQuery searchQuery) throws BlurException, TException {
		try {
		    HitsIterable hitsIterable = scatterGather(new BlurCommand<HitsIterable>() {
                @Override
                public HitsIterable call(Client client) throws Exception {
                    return new HitsIterableBlurClient(client,table,searchQuery);
                }
            },new MergerHitsIterable(searchQuery.minimumNumberOfHits,searchQuery.maxQueryTime));
			return BlurBaseServer.convertToHits(hitsIterable, searchQuery.start, searchQuery.fetch, searchQuery.minimumNumberOfHits);
		} catch (Exception e) {
			throw new LoggingBlurException(LOG,e,"Unknown error during search of [" +
                    "table=" + table + "searchquery=" + searchQuery + "]");
		}
	}
	
	@Override
	public FetchResult fetchRow(final String table, final Selector selector) throws BlurException,
			TException {
	    String clientHostnamePort = null;
		try {
		    clientHostnamePort = getNode(table,selector);
		    return client.execute(clientHostnamePort, 
		        new BlurCommand<FetchResult>() {
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
            scatter(new BlurCommand<Void>() {
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
            return scatterGather(new BlurCommand<List<SearchQueryStatus>>() {
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
    
    @Override
    public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value) throws BlurException, TException {
        try {
            return scatterGather(
                new BlurCommand<Long>() {
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
                new BlurCommand<Schema>() {
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
                new BlurCommand<List<String>>() {
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
                layout.put(table, ForkJoin.execute(executor, clusterStatus.getOnlineShardServers(), new ParallelCall<String,Map<String,String>>() {
                    @Override
                    public Map<String,String> call(final String hostnamePort) throws Exception {
                        return client.execute(hostnamePort, new BlurCommand<Map<String,String>>() {
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
    
    private <R> R scatterGather(final BlurCommand<R> command, Merger<R> merger) throws Exception {
        return ForkJoin.execute(executor, clusterStatus.getOnlineShardServers(), new ParallelCall<String, R>() {
            @SuppressWarnings("unchecked")
            @Override
            public R call(String hostnamePort) throws Exception {
                return client.execute(hostnamePort, (BlurCommand<R>) command.clone());
            }
        }).merge(merger);
    }
    
    private <R> void scatter(BlurCommand<R> command) throws Exception {
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
    public TableDescriptor describe(final String table) throws BlurException, TException {
        try {
            String hostname = getSingleOnlineShardServer();
            return client.execute(hostname, new BlurCommand<TableDescriptor>() {
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
            return client.execute(hostname, new BlurCommand<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.tableList();
                }
            });
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get table list.  Current online shard servers [" +
            		clusterStatus.getOnlineShardServers() + "]");
        }
    }
    
    private String getSingleOnlineShardServer() throws BlurException, TException {
        List<String> onlineShardServers = clusterStatus.getOnlineShardServers();
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

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        try {
            return clusterStatus.controllerServerList();
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get a controller list.");
        }
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        try {
            return clusterStatus.shardServerList();
        } catch (Exception e) {
            throw new LoggingBlurException(LOG,e,"Unknown error while trying to get a shard list.");
        }
    }

    public ClusterStatus getClusterStatus() {
        return clusterStatus;
    }

    public void setClusterStatus(ClusterStatus clusterStatus) {
        this.clusterStatus = clusterStatus;
    }
}