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

package com.nearinfinity.blur.manager.indexserver;

import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesCompressionBlockSize;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesCompressionCodec;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesEnabled;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesPath;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesShardCount;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.getBlurTablesUri;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.search.Similarity;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.indexserver.DistributedManager.Value;
import com.nearinfinity.blur.manager.writer.BlurIndex;

public abstract class AdminIndexServer extends AbstractIndexServer {
    
    private static final Log LOG = LogFactory.getLog(AdminIndexServer.class);

    protected String nodeName;
    protected AtomicReference<Map<String,TABLE_STATUS>> statusMap = new AtomicReference<Map<String,TABLE_STATUS>>(new HashMap<String, TABLE_STATUS>());
    protected AtomicReference<List<String>> tableList = new AtomicReference<List<String>>(new ArrayList<String>());
    protected AtomicReference<Map<String, BlurAnalyzer>> analyzerMap = new AtomicReference<Map<String, BlurAnalyzer>>(new HashMap<String, BlurAnalyzer>());
    protected AtomicReference<Map<String,List<String>>> shardServerExclusionMap = new AtomicReference<Map<String,List<String>>>(new HashMap<String,List<String>>());
    protected DistributedManager dm;
    protected Timer daemon;
    protected ExecutorService executorService;
    protected int threadCount = 32;
    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            updateStatus();
        }
    };
    
    /**
     * All sub classes need to call super.init().
     * @return 
     * @throws IOException 
     */
    public void init() {
        LOG.info("init - start");
        executorService = Executors.newThreadPool("admin-index-server",threadCount);
        dm.createPath(getBlurTablesPath()); //ensures the path exists
        updateStatus();
        startUpdateStatusPollingDaemon();
        LOG.info("init - complete");
    }
    
    public void close() {
        daemon.cancel();
        daemon.purge();
        executorService.shutdownNow();
    }

    protected void startUpdateStatusPollingDaemon() {
        daemon = new Timer("AdminIndexServer-Status-Poller", true);
        daemon.schedule(new TimerTask() {
            @Override
            public void run() {
                updateStatus();
            }
        }, 0, TimeUnit.SECONDS.toMillis(1));
    }

    protected synchronized void updateStatus() {
        updateTableList();
        updateTableAnalyzers();
        updateTableStatus();
        registerCallbackForChanges();
        warmUpIndexes();
    }

    protected void warmUpIndexes() {
        LOG.debug("Warm up indexes");
        List<String> tableList = getTableList();
        for (String t : tableList) {
            final String table = t;
            if (getTableStatus(table) == TABLE_STATUS.ENABLED) {
                LOG.debug("Warm up table [{0}]",t);
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        warmUpTable(table);
                    }
                });
            }
        }
    }

    protected void warmUpTable(String table) {
        try {
            LOG.debug("Warmup for table [{0}]",table);
            Map<String, BlurIndex> blurIndexes = getIndexes(table);
            LOG.debug("Warmup complete for table [{0}] shards [{1}]", table, blurIndexes.keySet());
        } catch (Exception e) {
            LOG.error("Warmup error with table [{0}]",e,table);
        }
    }

    protected void registerCallbackForChanges() {
        dm.registerCallableOnChange(watcher, getBlurTablesPath());
        for (String table : tableList.get()) {
            if (dm.exists(getBlurTablesPath(), table)) {
                dm.registerCallableOnChange(watcher, getBlurTablesPath(), table);
            } else {
                LOG.debug("Trying to register wather for path [" + getBlurTablesPath() + "/" + table + "] and it doesn't exist.");
            }
        }        
    }

    protected void updateTableList() {
        List<String> newTables = dm.list(getBlurTablesPath());
        List<String> oldTables = tableList.get();
        tableList.set(newTables);
        for (String table : newTables) {
            if (!oldTables.contains(table)) {
                LOG.info("Table [{0}] identified.",table);
            }
        }
        for (String table : oldTables) {
            if (!newTables.contains(table)) {
                LOG.info("Table [{0}] removed.",table);
            }
        }
    }
    
    protected void updateTableAnalyzers() {
        Map<String, BlurAnalyzer> newMap = new HashMap<String, BlurAnalyzer>();
        for (String table : tableList.get()) {
            Value value = new Value();
            dm.fetchData(value, getBlurTablesPath(), table);
            BlurAnalyzer analyzer;
            if (value.data == null) {
                analyzer = BlurAnalyzer.BLANK_ANALYZER;
            } else {
                try {
                    analyzer = BlurAnalyzer.create(new ByteArrayInputStream(value.data));
                } catch (IOException e) {
                    LOG.error("Error trying to load analyzer for table [{0}], using blank analyzer.",table);
                    analyzer = BlurAnalyzer.BLANK_ANALYZER;
                }
            }
            newMap.put(table, analyzer);
        }
        analyzerMap.set(newMap);
    }

    protected void updateTableStatus() {
        Map<String, TABLE_STATUS> newMap = new HashMap<String, TABLE_STATUS>();
        Map<String, TABLE_STATUS> oldMap = statusMap.get();
        List<String> tableStatusChanges = new ArrayList<String>();
        for (String table : tableList.get()) {
            TABLE_STATUS status;
            if (dm.exists(getBlurTablesPath(),table,getBlurTablesEnabled())) {
                status = TABLE_STATUS.ENABLED;
            } else {
                status = TABLE_STATUS.DISABLED;
            }
            newMap.put(table, status);
            TABLE_STATUS oldStatus = oldMap.get(table);
            if (oldStatus == null || oldStatus != status) {
                LOG.info("Table [{0}] change status to [{1}]",table,status);
                tableStatusChanges.add(table);
            }
        }
        statusMap.set(newMap);
        for (String table : oldMap.keySet()) {
            if (!newMap.containsKey(table)) {
                LOG.info("Status could not be found for table [{0}], possibly removed.",table);
            }
        }
        for (String table : tableStatusChanges) {
            tableStatusChanged(table);
        }
    }

    protected abstract void tableStatusChanged(String table);

    @Override
    public final BlurAnalyzer getAnalyzer(String table) {
        BlurAnalyzer analyzer = analyzerMap.get().get(table);
        if (analyzer == null) {
            return BlurAnalyzer.BLANK_ANALYZER;
        }
        return analyzer;
    }

    @Override
    public final String getNodeName() {
        return nodeName;
    }

    @Override
    public final Similarity getSimilarity(String table) {
        return new FairSimilarity();
    }

    @Override
    public final List<String> getTableList() {
        return tableList.get();
    }
    
    @Override
    public final TABLE_STATUS getTableStatus(String table) {
        TABLE_STATUS tableStatus = statusMap.get().get(table);
        if (tableStatus == null) {
            return TABLE_STATUS.DISABLED;
        }
        return tableStatus;
    }
    
    @Override
    public String getTableUri(String table) {
        Value value = new Value();
        dm.fetchData(value, getBlurTablesPath(),table,getBlurTablesUri());
        return new String(value.data);
    }
    
    @Override
    public int getShardCount(String table) {
        Value value = new Value();
        dm.fetchData(value, getBlurTablesPath(), table, getBlurTablesShardCount());
        return Integer.parseInt(new String(value.data));
    }
    
    @Override
    public int getCompressionBlockSize(String table) {
        Value value = new Value();
        dm.fetchData(value, getBlurTablesPath(), table, getBlurTablesCompressionBlockSize());
        return Integer.parseInt(new String(value.data));
    }

    @Override
    public CompressionCodec getCompressionCodec(String table) {
        Value value = new Value();
        dm.fetchData(value, getBlurTablesPath(), table, getBlurTablesCompressionCodec());
        return getInstance(new String(value.data));
    }

    private CompressionCodec getInstance(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (CompressionCodec) clazz.newInstance();
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get instance of [" + className + "]", e);
            throw new RuntimeException(e);
        }
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setDistributedManager(DistributedManager distributedManager) {
        this.dm = distributedManager;
    }
}
