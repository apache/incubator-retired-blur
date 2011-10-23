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

package com.nearinfinity.blur.manager.clusterstatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class ZookeeperClusterStatus extends ClusterStatus {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);
    
    private ZooKeeper _zk;
    private AtomicReference<List<String>> _clusters = new AtomicReference<List<String>>();
    private AtomicReference<List<String>> _controllers = new AtomicReference<List<String>>();
    private Map<String,AtomicReference<List<String>>> _onlineShardServers = new ConcurrentHashMap<String, AtomicReference<List<String>>>();
    private Map<String,AtomicReference<List<String>>> _shardServers = new ConcurrentHashMap<String, AtomicReference<List<String>>>();
    private Map<String,AtomicReference<List<String>>> _tables = new ConcurrentHashMap<String, AtomicReference<List<String>>>();
    private AtomicBoolean _running = new AtomicBoolean(true);
    private List<Thread> _threads = new ArrayList<Thread>();
    
    public static void main(String[] args) throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper("localhost", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                
            }
        });
        ZookeeperClusterStatus status = new ZookeeperClusterStatus(zooKeeper);
        for (int i = 0; i < 100; i++) {
            System.out.println(status.getClusterList());
            System.out.println(status.getControllerServerList());
            System.out.println(status.getOnlineShardServers("default"));
            System.out.println(status.getShardServerList("default"));
            System.out.println(status.getTableList());
            
            for (String cluster : status.getClusterList()) {
                System.out.println(status.getOnlineShardServers(cluster));
                System.out.println(status.getShardServerList(cluster));
            }
            for (String table : status.getTableList()) {
                System.out.println(status.getTableDescriptor(table));
            }
        }
    }
    
    public ZookeeperClusterStatus(ZooKeeper zooKeeper) {
        _zk = zooKeeper;
        watch(ZookeeperPathConstants.getBlurClusterPath(),_clusters,"Clusters");
        watch(ZookeeperPathConstants.getBlurOnlineControllersPath(),_controllers,"Controllers");
        getTableList();//warmup cluster to table mapping
    }
    
    @Override
    public List<String> getClusterList() {
        return _clusters.get();
    }

    @Override
    public List<String> getControllerServerList() {
        return _controllers.get();
    }

    @Override
    public synchronized List<String> getOnlineShardServers(String cluster) {
        AtomicReference<List<String>> online = _onlineShardServers.get(cluster);
        if (online == null) {
            online = new AtomicReference<List<String>>();
            _onlineShardServers.put(cluster, online);
            String path = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/online/shard-nodes";
            watch(path,online,"OnlineShardsInCluster-" + cluster);
        }
        return online.get();
    }

    @Override
    public List<String> getShardServerList(String cluster) {
        AtomicReference<List<String>> shards = _shardServers.get(cluster);
        if (shards == null) {
            shards = new AtomicReference<List<String>>();
            _shardServers.put(cluster, shards);
            String path = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/shard-nodes";
            watch(path,shards,"ShardsInCluster-" + cluster);
        }
        return shards.get();
    }
    
    @Override
    public boolean exists(String table) {
      String cluster = getCluster(table);
      if (cluster == null) {
          return false;
      }
      return true;
    }

    @Override
    public boolean isEnabled(String table) {
      String cluster = getCluster(table);
      if (cluster == null) {
          return false;
      }
      String tablePathIsEnabled = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table + "/" + ZookeeperPathConstants.getBlurTablesEnabled();
      try {
        if (_zk.exists(tablePathIsEnabled, false) == null) {
          return false;
        }
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
          throw new RuntimeException(e);
      }
      return true;
    }

    @Override
    public TableDescriptor getTableDescriptor(String table) {
        String cluster = getCluster(table);
        if (cluster == null) {
            return null;
        }
        String tablePath = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables/" + table;
        TableDescriptor tableDescriptor = new TableDescriptor();
        try {
            if (_zk.exists(tablePath + "/enabled", false) == null) {
                tableDescriptor.isEnabled = false;
            } else {
                tableDescriptor.isEnabled = true;
            }
            tableDescriptor.shardCount = Integer.parseInt(new String(getData(tablePath + "/shard-count")));
            tableDescriptor.tableUri = new String(getData(tablePath + "/uri"));
            tableDescriptor.compressionClass = new String(getData(tablePath + "/compression-codec"));
            tableDescriptor.compressionBlockSize = Integer.parseInt(new String(getData(tablePath + "/compression-blocksize")));
            tableDescriptor.analyzerDefinition = getAnalyzerDefinition(getData(tablePath));
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        tableDescriptor.cluster = cluster;
        return tableDescriptor;
    }
    
    private AnalyzerDefinition getAnalyzerDefinition(byte[] data) {
        TMemoryInputTransport trans = new TMemoryInputTransport(data);
        TJSONProtocol protocol = new TJSONProtocol(trans);
        AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
        try {
            analyzerDefinition.read(protocol);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        trans.close();
        return analyzerDefinition;
    }

    private byte[] getData(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat == null) {
            return null;
        }
        return _zk.getData(path, false, stat);
    }

    @Override
    public List<String> getTableList() {
        List<String> result = new ArrayList<String>();
        for (String cluster : getClusterList()) {
            AtomicReference<List<String>> tables = _tables.get(cluster);
            if (tables == null) {
                tables = new AtomicReference<List<String>>();
                _tables.put(cluster, tables);
                String path = ZookeeperPathConstants.getBlurClusterPath() + "/" + cluster + "/tables";
                watch(path,tables,"TablesInCluster-" + cluster);
            }
            result.addAll(tables.get());
        }
        return result;
    }
    
    public void close() {
        _running.set(false);
        for (Thread thread : _threads) {
            thread.interrupt();
        }
    }
    
    private void watch(final String path, final AtomicReference<List<String>> list, String name) {
        Thread thread = new Thread(new Runnable() {
            private Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    synchronized (list) {
                        list.notifyAll();
                    }
                }
            };
            
            @Override
            public void run() {
                while (_running.get()) {
                    try {
                        List<String> before = list.get();
                        List<String> after = _zk.getChildren(path, watcher);
                        list.set(after);
                        synchronized (list) {
                            LOG.info("Children for [{0}] changed to [{1}] from [{2}]",path,after,before);
                            list.wait();
                        }
                    } catch (KeeperException e) {
                        LOG.error("Unknown error.",e);
                        return;
                    } catch (InterruptedException e) {
                        LOG.error("Unknown error.",e);
                        return;
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.setName("Watching-" + name);
        thread.start();
        while (list.get() == null) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOG.error("Unknown error.",e);
                return;
            }
        }
        _threads.add(thread);
    }

    @Override
    public String getCluster(String table) {
        Map<String,AtomicReference<List<String>>> tables = new HashMap<String, AtomicReference<List<String>>>(_tables);
        for (Entry<String,AtomicReference<List<String>>> entry : tables.entrySet()) {
            List<String> tablesInCluster = new ArrayList<String>(entry.getValue().get());
            if (tablesInCluster.contains(table)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
