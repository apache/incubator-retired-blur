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
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.indexserver.ClusterStatus;
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
import com.nearinfinity.blur.thrift.generated.Transaction;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.ForkJoin;
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

    public void open() {
        _executor = Executors.newThreadPool(CONTROLLER_THREAD_POOL, _threadCount);
        updateShardLayout();
        _shardLayoutTimer = new Timer("Shard-Layout-Timer", true);
        _shardLayoutTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateShardLayout();
            }
        }, _layoutDelay, _layoutDelay);
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
        try {
            final AtomicLongArray facetCounts = BlurUtil.getAtomicLongArraySameLengthAsList(blurQuery.facets);

            Selector selector = blurQuery.getSelector();
            blurQuery.setSelector(null);

            BlurResultIterable hitsIterable = scatterGather(new BlurCommand<BlurResultIterable>() {
                @Override
                public BlurResultIterable call(Client client) throws Exception {
                    return new BlurResultIterableClient(client, table, blurQuery, facetCounts);
                }
            }, new MergerBlurResultIterable(blurQuery));
            return BlurBaseServer.convertToHits(hitsIterable, blurQuery, facetCounts, _executor, selector, this, table);
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
            scatter(new BlurCommand<Void>() {
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
            return scatterGather(new BlurCommand<List<BlurQueryStatus>>() {
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
    		return scatterGather(new BlurCommand<TableStats>() {

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
            return scatterGather(new BlurCommand<Long>() {
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
            return scatterGather(new BlurCommand<Schema>() {
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
            return scatterGather(new BlurCommand<List<String>>() {
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
            LOG
                    .error(
                            "Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
                            e, table, columnFamily, columnName, startWith, size);
            throw new BException(
                    "Unknown error while trying to terms table [{0}] columnFamily [{1}] columnName [{2}] startWith [{3}] size [{4}]",
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
                layout.put(table, ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(),
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

    private <R> R scatterGather(final BlurCommand<R> command, Merger<R> merger) throws Exception {
        return ForkJoin.execute(_executor, _clusterStatus.getOnlineShardServers(), new ParallelCall<String, R>() {
            @SuppressWarnings("unchecked")
            @Override
            public R call(String hostnamePort) throws Exception {
                return _client.execute(hostnamePort, (BlurCommand<R>) command.clone());
            }
        }).merge(merger);
    }

    private <R> void scatter(BlurCommand<R> command) throws Exception {
        scatterGather(command, new Merger<R>() {
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
            return _client.execute(hostname, new BlurCommand<TableDescriptor>() {
                @Override
                public TableDescriptor call(Client client) throws Exception {
                    return client.describe(table);
                }
            });
        } catch (Exception e) {
            LOG.error("Unknown error while trying to describe table [{0}]", e, table);
            throw new BException("Unknown error while trying to describe table [{0}]", e, table);
        }
    }

    @Override
    public List<String> tableList() throws BlurException, TException {
        try {
            String hostname = getSingleOnlineShardServer();
            return _client.execute(hostname, new BlurCommand<List<String>>() {
                @Override
                public List<String> call(Client client) throws Exception {
                    return client.tableList();
                }
            });
        } catch (Exception e) {
            List<String> onlineShardServers = _clusterStatus.getOnlineShardServers();
            LOG.error("Unknown error while trying to get table list.  Current online shard servers [{0}]", e,
                    onlineShardServers);
            throw new BException("Unknown error while trying to get table list.  Current online shard servers [{0}]",
                    e, onlineShardServers);
        }
    }

    private String getSingleOnlineShardServer() throws BlurException, TException {
        List<String> onlineShardServers = _clusterStatus.getOnlineShardServers();
        return onlineShardServers.get(_random.nextInt(onlineShardServers.size()));
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
            return _clusterStatus.controllerServerList();
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get a controller list.", e);
            throw new BException("Unknown error while trying to get a controller list.", e);
        }
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        try {
            return _clusterStatus.shardServerList();
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get a shard list.", e);
            throw new BException("Unknown error while trying to get a shard list.", e);
        }
    }

    public ClusterStatus getClusterStatus() {
        return _clusterStatus;
    }

    public void setClusterStatus(ClusterStatus clusterStatus) {
        this._clusterStatus = clusterStatus;
    }

    @Override
    public void mutate(final String table, final Transaction transaction, List<RowMutation> mutations) throws BlurException, TException {
        try {
            final Map<String, List<RowMutation>> mutationsMap = getMutationMap(table, mutations);
            for (String hname : mutationsMap.keySet()) {
                final String hostname = hname;
                final List<RowMutation> mutationsLst = mutationsMap.get(hostname);
                _client.execute(hostname, new BlurCommand<Void>() {
                    @Override
                    public Void call(Client client) throws Exception {
                        client.mutate(table, transaction, mutationsLst);
                        return null;
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Unknown error during mutate of table [{0}]", e, table);
            throw new BException("Unknown error during mutate of table [{0}]", e, table);
        }
    }

    private Map<String, List<RowMutation>> getMutationMap(String table, List<RowMutation> mutations) throws BlurException, TException {
        int numberOfShards = getShardCount(table);
        Map<String, String> tableLayout = _shardServerLayout.get().get(table);
        if (tableLayout.size() != numberOfShards) {
            throw new BlurException("Cannot update data while shard is missing",null);
        }
        Map<String, List<RowMutation>> mutationsMap = new HashMap<String, List<RowMutation>>();
        for (RowMutation mutation : mutations) {
            MutationHelper.validateMutation(mutation);
            String shardName = MutationHelper.getShardName(table, mutation.rowId, numberOfShards, _blurPartitioner);
            String node = tableLayout.get(shardName);
            List<RowMutation> list = mutationsMap.get(node);
            if (list == null) {
                list = new ArrayList<RowMutation>();
                mutationsMap.put(node, list);
            }
            list.add(mutation);
        }
        return mutationsMap;
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
    public void mutateAbort(String table, Transaction transaction) throws BlurException, TException {
        throw new RuntimeException("not impl");
    }

    @Override
    public void mutateCommit(String table, Transaction transaction) throws BlurException, TException {
        throw new RuntimeException("not impl");
    }

    @Override
    public Transaction mutateCreateTransaction(String table) throws BlurException, TException {
        throw new RuntimeException("not impl");
    }

}