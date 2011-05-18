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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.manager.writer.BlurIndexReader;

public abstract class DistributedIndexServer extends AdminIndexServer {
    
    private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);

    private static final IndexReader EMPTY_INDEXREADER;
    private static final BlurIndex EMPTY_BLURINDEX;
    
    static {
        RAMDirectory directory = new RAMDirectory();
        try {
            new IndexWriter(directory,new KeywordAnalyzer(),MaxFieldLength.UNLIMITED).close();
            EMPTY_INDEXREADER = IndexReader.open(directory);
            EMPTY_BLURINDEX = new BlurIndexReader(EMPTY_INDEXREADER);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private ConcurrentHashMap<String,Map<String,BlurIndex>> readers = new ConcurrentHashMap<String, Map<String,BlurIndex>>();
    private Timer readerCloserDaemon;
    private long delay = TimeUnit.SECONDS.toMillis(30);
    private Map<String,DistributedLayoutManager> layoutManagers = new ConcurrentHashMap<String, DistributedLayoutManager>();
    private Map<String, Set<String>> layoutCache = new ConcurrentHashMap<String, Set<String>>();
    //need a GC daemon for closing indexes
    //need a daemon for tracking what indexes to open ???
    //need a daemon to track reopening changed indexes
    
    public void init() {
        super.init();
        startIndexReaderCloserDaemon();
    }
    
    protected abstract BlurIndex openShard(String table, String shard) throws IOException;
    
    protected abstract void beforeClose(String shard, BlurIndex index);
    
    public void shardServerStateChange() {
        layoutManagers.clear();
        layoutCache.clear();
    }

    @Override
    public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        Set<String> shardsToServe = getShardsToServe(table);
        setupReaders(table);
        Map<String, BlurIndex> tableIndexes = readers.get(table);
        Set<String> shardsBeingServed = new HashSet<String>(tableIndexes.keySet());
        if (shardsBeingServed.containsAll(shardsToServe)) {
            Map<String, BlurIndex> result = new HashMap<String, BlurIndex>(tableIndexes);
            shardsBeingServed.removeAll(shardsToServe);
            for (String shardNotToServe : shardsBeingServed) {
                result.remove(shardNotToServe);
            }
            return result;
        } else {
            return openMissingShards(table,shardsToServe,tableIndexes);
        }
    }
    
    protected void closeOldReaders() {
        List<String> tableList = getTableList();
        for (String table : tableList) {
            closeOldReaders(table);
        }
        closeOldReadersForTablesThatAreMissing();
    }
    
    private void closeOldReadersForTablesThatAreMissing() {
        Set<String> tablesWithOpenIndexes = readers.keySet();
        List<String> currentTables = getTableList();
        for (String table : tablesWithOpenIndexes) {
            if (!currentTables.contains(table)) {
                //close all readers
                LOG.info("Table [" + table + "] no longer available, closing all indexes.");
                Map<String, BlurIndex> map = readers.get(table);
                for (String shard : map.keySet()) {
                    closeIndex(table, shard, map.get(shard));
                }
                readers.remove(table);
            }
        }
    }

    protected void closeOldReaders(String table) {
        Set<String> shardsToServe = getShardsToServe(table);
        Map<String, BlurIndex> tableIndex = readers.get(table);
        if (tableIndex == null) {
            return;
        }
        Set<String> shardsOpen = new HashSet<String>(tableIndex.keySet());
        shardsOpen.removeAll(shardsToServe);
        if (shardsOpen.isEmpty()) {
            return;
        }
        for (String shard : shardsOpen) {
            BlurIndex index = tableIndex.remove(shard);
            closeIndex(table, shard, index);
        }
    }

    private void closeIndex(String table, String shard, BlurIndex index) {
        try {
            beforeClose(shard,index);
            index.close();
            LOG.info("Index for table [{0}] shard [{1}] closed.",table,shard);
        } catch (Exception e) {
            LOG.error("Error while closing index reader [{0}]",e,index);
        }
    }

    @Override
    public void close() {
        super.close();
    }
    
    //Getters and setters

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }
    
    //Getters and setters
    
    private synchronized Map<String, BlurIndex> openMissingShards(final String table, Set<String> shardsToServe, final Map<String, BlurIndex> tableIndexes) {
        Map<String, BlurIndex> result = new HashMap<String, BlurIndex>();
        for (String s : shardsToServe) {
            final String shard = s;
            BlurIndex blurIndex = tableIndexes.get(shard);
            if (blurIndex == null) {
                try {
                    tableIndexes.put(shard, openShard(table,shard));
                } catch (Exception e) {
                    LOG.error("Unknown error while opening shard [{0}] for table [{1}].",e.getCause(),shard,table);
                    result.put(shard, EMPTY_BLURINDEX);
                }
            }
            result.put(shard, blurIndex);
        }
        return result;
    }

    private void setupReaders(String table) {
        readers.putIfAbsent(table, new ConcurrentHashMap<String, BlurIndex>());
    }
    
    private Set<String> getShardsToServe(String table) {
        DistributedLayoutManager layoutManager = layoutManagers.get(table);
        if (layoutManager == null) {
            return setupLayoutManager(table);
        } else {
            return layoutCache.get(table);
        }
    }
    
    private synchronized Set<String> setupLayoutManager(String table) {
        DistributedLayoutManager layoutManager = new DistributedLayoutManager();
        
        List<String> shardServerList = getShardServerList();
        List<String> offlineShardServers = new ArrayList<String>(getOfflineShardServers());
        //add exclusions to offline list
        offlineShardServers.addAll(getShardServerExclusion(table));
        List<String> shardList = getShardList(table);
        
        layoutManager.setNodes(shardServerList);
        layoutManager.setNodesOffline(offlineShardServers);
        layoutManager.setShards(shardList);
        layoutManager.init();
        
        Map<String, String> layout = layoutManager.getLayout();
        String nodeName = getNodeName();
        Set<String> shardsToServeCache = new TreeSet<String>();
        for (Entry<String,String> entry : layout.entrySet()) {
            if (entry.getValue().equals(nodeName)) {
                shardsToServeCache.add(entry.getKey());
            }
        }
        layoutCache.put(table, shardsToServeCache);
        layoutManagers.put(table, layoutManager);
        return shardsToServeCache;
    }

    //Daemon threads

    private void startIndexReaderCloserDaemon() {
        readerCloserDaemon = new Timer("Reader-Closer-Daemon", true);
        readerCloserDaemon.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                closeOldReaders();
            }
        }, delay, delay);
    }
}
