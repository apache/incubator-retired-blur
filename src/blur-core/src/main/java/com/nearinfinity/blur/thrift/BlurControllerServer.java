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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.BlurPartitioner;
import com.nearinfinity.blur.manager.BlurQueryChecker;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.results.BlurResultIterableClient;
import com.nearinfinity.blur.manager.results.MergerBlurResultIterable;
import com.nearinfinity.blur.manager.stats.MergerTableStats;
import com.nearinfinity.blur.manager.status.MergerQueryStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.QueryCache;
import com.nearinfinity.blur.utils.QueryCacheEntry;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurControllerServer extends TableAdmin implements Iface {

    private static final String CONTROLLER_THREAD_POOL = "controller-thread-pool";
    private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);

    private ExecutorService _executor;
    private AtomicReference<Map<String, Map<String, String>>> _shardServerLayout = new AtomicReference<Map<String, Map<String, String>>>(
            new HashMap<String, Map<String, String>>());
    private BlurClient _client;
    private long _layoutDelay = TimeUnit.SECONDS.toMillis(5);
    private Random _random = new Random();
    private Timer _shardLayoutTimer;
    private ClusterStatus _clusterStatus;
    private int _threadCount = 64;
    private boolean _closed;
    private Map<String, Integer> _tableShardCountMap = new ConcurrentHashMap<String, Integer>();
    private BlurPartitioner<BytesWritable, Void> _blurPartitioner = new BlurPartitioner<BytesWritable, Void>();
    private String _nodeName;
    private int _remoteFetchCount = 100;
    private long _maxTimeToLive = TimeUnit.MINUTES.toMillis(1);
    private int _maxQueryCacheElements = 128;
    private QueryCache _queryCache;
    private BlurQueryChecker _queryChecker;

    public void init() {
        _queryCache = new QueryCache("controller-cache",_maxQueryCacheElements,_maxTimeToLive);
        _executor = Executors.newThreadPool(CONTROLLER_THREAD_POOL, _threadCount);
        updateShardLayout();
        _shardLayoutTimer = new Timer("Shard-Layout-Timer", true);
        _shardLayoutTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateShardLayout();
            }
        }, _layoutDelay, _layoutDelay);
        _dm.createEphemeralPath(ZookeeperPathConstants.getBlurOnlineControllersPath() + "/" + _nodeName);
    }

    public synchronized void close() {
        if (!_closed) {
            _closed = true;
            _shardLayoutTimer.cancel();
            _executor.shutdownNow();
        }
    }

    @Override
    public BlurResults query(final String table, final BlurQuery blurQuery) throws BlurException, TException {
        _queryChecker.checkQuery(blurQuery);
        try {
            final AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);

            BlurQuery original = new BlurQuery(blurQuery);
            if (blurQuery.useCacheIfPresent) {
                LOG.debug("Using cache for query [{0}] on table [{1}].",blurQuery, table);
                BlurQuery noralizedBlurQuery = _queryCache.getNormalizedBlurQuery(blurQuery);
                QueryCacheEntry queryCacheEntry = _queryCache.get(noralizedBlurQuery);
                if (_queryCache.isValid(queryCacheEntry)) {
                    LOG.debug("Cache hit for query [{0}] on table [{1}].",blurQuery, table);
                    return queryCacheEntry.getBlurResults(blurQuery);
                }
            }
            
            Selector selector = blurQuery.getSelector();
            blurQuery.setSelector(null);

            BlurResultIterable hitsIterable = scatterGather(getCluster(table), new BlurCommand<BlurResultIterable>() {
                @Override
                public BlurResultIterable call(Client client) throws Exception {
                    return new BlurResultIterableClient(client, table, blurQuery, facetCounts, _remoteFetchCount);
                }
            }, new MergerBlurResultIterable(blurQuery));
            return _queryCache.cache(original, BlurUtil.convertToHits(hitsIterable, blurQuery, facetCounts, _executor, selector, this, table));
        } catch (Exception e) {
            LOG.error("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
            throw new BException("Unknown error during search of [table={0},blurQuery={1}]", e, table, blurQuery);
        }
    }

    @Override
    public FetchResult fetchRow(final String table, final Selector selector) throws BlurException, TException {
        IndexManager.validSelector(selector);
        String clientHostnamePort = null;
        try {
            clientHostnamePort = getNode(table, selector);
            return _client.execute(clientHostnamePort, new BlurCommand<FetchResult>() {
                @Override
                public FetchResult call(Client client) throws Exception {
                    return client.fetchRow(table, selector);
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e, table,
                    selector, clientHostnamePort);
            throw new BException("Unknown error during fetch of row from table [{0}] selector [{1}] node [{2}]", e,
                    table, selector, clientHostnamePort);
        }
    }

    @Override
    public void cancelQuery(final String table, final long uuid) throws BlurException, TException {
        try {
            scatter(getCluster(table), new BlurCommand<Void>() {
                @Override
                public Void call(Client client) throws Exception {
                    client.cancelQuery(table, uuid);
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
            throw new BException("Unknown error while trying to cancel search table [{0}] uuid [{1}]", e, table, uuid);
        }
    }

    @Override
    public List<BlurQueryStatus> currentQueries(final String table) throws BlurException, TException {
        try {
            return scatterGather(getCluster(table), new BlurCommand<List<BlurQueryStatus>>() {
                @Override
                public List<BlurQueryStatus> call(Client client) throws Exception {
                    return client.currentQueries(table);
                }
            }, new MergerQueryStatus());
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get current searches [{0}]", e, table);
            throw new BException("Unknown error while trying to get current searches [{0}]", e, table);
        }
    }
    
    @Override
    public TableStats getTableStats(final String table) throws BlurException, TException {
    	try {
    		return scatterGather(getCluster(table), new BlurCommand<TableStats>() {

				@Override
				public TableStats call(Client client) throws Exception {
					return client.getTableStats(table);
				}
    			
    		}, new MergerTableStats());
    	} catch (Exception e) {
            LOG.error("Unknown error while trying to get table stats [{0}]", e, table);
            throw new BException("Unknown error while trying to get table stats [{0}]", e, table);
        }
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        Map<String, Map<String, String>> layout = _shardServerLayout.get();
        Map<String, String> tableLayout = layout.get(table);
        if (tableLayout == null) {
            return new HashMap<String, String>();
        }
        return tableLayout;
    }

    @Override
    public long recordFrequency(final String table, final String columnFamily, final String columnName,
            final String value) throws BlurException, TException {
        try {
            return scatterGather(getCluster(table), new BlurCommand<Long>() {
                @Override
                public Long call(Client client) throws Exception {
                    return client.recordFrequency(table, columnFamily, columnName, value);
                }
            }, new Merger<Long>() {
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
            LOG.error("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table, columnFamily,
                    columnName, value);
            throw new BException("Unknown error while trying to get record frequency [{0}/{1}/{2}/{3}]", e, table,
                    columnFamily, columnName, value);
        }
    }

    @Override
    public Schema schema(final String table) throws BlurException, TException {
        try {
            return scatterGather(getCluster(table), new BlurCommand<Schema>() {
                @Override
                public Schema call(Client client) throws Exception {
                    return client.schema(table);
                }
            }, new Merger<Schema>() {
                @Override
                public Schema merge(BlurExecutorCompletionService<Schema> service) throws Exception {
                    Schema result = null;
                    while (service.getRemainingCount() > 0) {
                        Schema schema = service.take().get();
                        if (result == null) {
                            result = schema;
                        } else {
                            result = BlurControllerServer.merge(result, schema);
                        }
                    }
                    return result;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to schema table [{0}]", e, table);
            throw new BException("Unknown error while trying to schema table [{0}]", e, table);
        }
    }

    @Override
    public List<String> terms(final String table, final String columnFamily, final String columnName,
            final String startWith, final short size) throws BlurException, TException {
        try {
            return scatterGather(getCluster(table), new BlurCommand<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.terms(table, columnFamily, columnName, startWith, size);
                }
            }, new Merger<List<String>>() {
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
            LOG.error("Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
                    e, table, columnFamily, columnName, startWith, size);
            throw new BException("Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
                    e, table, columnFamily, columnName, startWith, size);
        }
    }

    public BlurClient getClient() {
        return _client;
    }

    public void setClient(BlurClient client) {
        this._client = client;
    }

    private String getNode(String table, Selector selector) throws BlurException, TException {
        Map<String, String> layout = shardServerLayout(table);
        String locationId = selector.locationId;
        if (locationId != null) {
            String shard = locationId.substring(0, locationId.indexOf('/'));
            return layout.get(shard);
        }
        int numberOfShards = getShardCount(table);
        if (selector.rowId != null) {
            String shardName = MutationHelper.getShardName(table, selector.rowId, numberOfShards, _blurPartitioner);
            return layout.get(shardName);
        }
        throw new BlurException("Selector is missing both a locationid and a rowid, one is needed.",null);
    }

    private void updateShardLayout() {
        try {
            Map<String, Map<String, String>> layout = new HashMap<String, Map<String, String>>();
            for (String t : tableList()) {
                final String table = t;
                layout.put(table, ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(getCluster(table)),
                        new ParallelCall<String, Map<String, String>>() {
                            @Override
                            public Map<String, String> call(final String hostnamePort) throws Exception {
                                return _client.execute(hostnamePort, new BlurCommand<Map<String, String>>() {
                                    @Override
                                    public Map<String, String> call(Client client) throws Exception {
                                        return client.shardServerLayout(table);
                                    }
                                });
                            }
                        }).merge(new Merger<Map<String, String>>() {
                    @Override
                    public Map<String, String> merge(BlurExecutorCompletionService<Map<String, String>> service)
                            throws Exception {
                        Map<String, String> result = new HashMap<String, String>();
                        while (service.getRemainingCount() > 0) {
                            result.putAll(service.take().get());
                        }
                        return result;
                    }
                }));
            }
            _shardServerLayout.set(layout);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to update shard layout.", e);
        }
    }

    private <R> R scatterGather(String cluster, final BlurCommand<R> command, Merger<R> merger) throws Exception {
        return ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(cluster), new ParallelCall<String, R>() {
            @SuppressWarnings("unchecked")
            @Override
            public R call(String hostnamePort) throws Exception {
                return _client.execute(hostnamePort, (BlurCommand<R>) command.clone());
            }
        }).merge(merger);
    }

    private <R> void scatter(String cluster, BlurCommand<R> command) throws Exception {
        scatterGather(cluster, command, new Merger<R>() {
            @Override
            public R merge(BlurExecutorCompletionService<R> service) throws Exception {
                while (service.getRemainingCount() > 0) {
                    service.take().get();
                }
                return null;
            }
        });
    }
    
    private String getCluster(String table) throws BlurException, TException {
        TableDescriptor describe = describe(table);
        if (describe == null) {
            throw new BlurException("Table [" + table + "] not found.",null);
        }
        return describe.cluster;
    }

    @Override
    public TableDescriptor describe(final String table) throws BlurException, TException {
        try {
            return _clusterStatus.getTableDescriptor(table);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to describe a table [" + table + "].", e);
            throw new BException("Unknown error while trying to describe a table [" + table + "].", e);
        }
    }

    @Override
    public List<String> tableList() throws BlurException, TException {
        try {
            return _clusterStatus.getTableList();
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get a table list.", e);
            throw new BException("Unknown error while trying to get a table list.", e);
        }
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
            return _clusterStatus.getControllerServerList();
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get a controller list.", e);
            throw new BException("Unknown error while trying to get a controller list.", e);
        }
    }

    @Override
    public List<String> shardServerList(String cluster) throws BlurException, TException {
        if (cluster.equals(BlurConstants.BLUR_CLUSTER)) {
            try {
                return _clusterStatus.getShardServerList(cluster);
            } catch (Exception e) {
                LOG.error("Unknown error while trying to get a shard list.", e);
                throw new BException("Unknown error while trying to get a shard list.", e);
            }
        }
        throw new BException("Cluster [" + cluster + "] is not valid.");
    }

    public ClusterStatus getClusterStatus() {
        return _clusterStatus;
    }

    public void setClusterStatus(ClusterStatus clusterStatus) {
        this._clusterStatus = clusterStatus;
    }

    @Override
    public void mutate(final RowMutation mutation) throws BlurException, TException {
        try {
            MutationHelper.validateMutation(mutation);
            String table = mutation.getTable();
            
            int numberOfShards = getShardCount(table);
            Map<String, String> tableLayout = _shardServerLayout.get().get(table);
            if (tableLayout.size() != numberOfShards) {
                throw new BlurException("Cannot update data while shard is missing",null);
            }
            
            String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
            String node = tableLayout.get(shardName);
            _client.execute(node, new BlurCommand<Void>() {
                @Override
                public Void call(Client client) throws Exception {
                    client.mutate(mutation);
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error during mutation of [{0}]", e, mutation);
            throw new BException("Unknown error during mutation of [{0}]", e, mutation);
        }
    }

    private int getShardCount(String table) throws BlurException, TException {
        Integer numberOfShards = _tableShardCountMap.get(table);
        if (numberOfShards == null) {
            TableDescriptor descriptor = describe(table);
            numberOfShards = descriptor.shardCount;
            _tableShardCountMap.put(table, numberOfShards);
        }
        return numberOfShards;
    }

    @Override
    public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
        for (RowMutation mutation : mutations) {
            MutationHelper.validateMutation(mutation);
        }
        for (RowMutation mutation : mutations) {
            mutate(mutation);
        }
    }

    @Override
    public List<String> shardClusterList() throws BlurException, TException {
        return Arrays.asList(BlurConstants.BLUR_CLUSTER);
    }

    public void setNodeName(String nodeName) {
        _nodeName = nodeName;
    }

    public int getRemoteFetchCount() {
        return _remoteFetchCount;
    }

    public void setRemoteFetchCount(int remoteFetchCount) {
        _remoteFetchCount = remoteFetchCount;
    }
    
    public long getMaxTimeToLive() {
        return _maxTimeToLive;
    }

    public void setMaxTimeToLive(long maxTimeToLive) {
        _maxTimeToLive = maxTimeToLive;
    }

    public int getMaxQueryCacheElements() {
        return _maxQueryCacheElements;
    }

    public void setMaxQueryCacheElements(int maxQueryCacheElements) {
        _maxQueryCacheElements = maxQueryCacheElements;
    }
    
    public void setQueryChecker(BlurQueryChecker queryChecker) {
        _queryChecker = queryChecker;
    }
}