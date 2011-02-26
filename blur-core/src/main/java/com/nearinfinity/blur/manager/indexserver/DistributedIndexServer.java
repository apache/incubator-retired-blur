package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public abstract class DistributedIndexServer extends AdminIndexServer {
    
    private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);

    private static final IndexReader EMPTY_INDEXREADER;
    
    static {
        RAMDirectory directory = new RAMDirectory();
        try {
            new IndexWriter(directory,new KeywordAnalyzer(),MaxFieldLength.UNLIMITED).close();
            EMPTY_INDEXREADER = IndexReader.open(directory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private ConcurrentHashMap<String,Map<String,IndexReader>> readers = new ConcurrentHashMap<String, Map<String,IndexReader>>();
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
    
    protected abstract IndexReader openShard(String table, String shard) throws IOException;
    
    protected abstract void beforeClose(String shard, IndexReader indexReader);
    
    public void shardServerStateChange() {
        layoutManagers.clear();
        layoutCache.clear();
    }

    @Override
    public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
        Set<String> shardsToServe = getShardsToServe(table);
        setupReaders(table);
        Map<String, IndexReader> tableReaders = readers.get(table);
        Set<String> shardsBeingServed = new HashSet<String>(tableReaders.keySet());
        if (shardsBeingServed.containsAll(shardsToServe)) {
            Map<String, IndexReader> result = new HashMap<String, IndexReader>(tableReaders);
            shardsBeingServed.removeAll(shardsToServe);
            for (String shardNotToServe : shardsBeingServed) {
                result.remove(shardNotToServe);
            }
            return result;
        } else {
            return openMissingShards(table,shardsToServe,tableReaders);
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
                Map<String, IndexReader> map = readers.get(table);
                for (String shard : map.keySet()) {
                    closeIndex(table, shard, map.get(shard));
                }
                readers.remove(table);
            }
        }
    }

    protected void closeOldReaders(String table) {
        Set<String> shardsToServe = getShardsToServe(table);
        Map<String, IndexReader> tableReaders = readers.get(table);
        if (tableReaders == null) {
            return;
        }
        Set<String> shardsOpen = new HashSet<String>(tableReaders.keySet());
        shardsOpen.removeAll(shardsToServe);
        if (shardsOpen.isEmpty()) {
            return;
        }
        for (String shard : shardsOpen) {
            IndexReader indexReader = tableReaders.remove(shard);
            closeIndex(table, shard, indexReader);
        }
    }

    private void closeIndex(String table, String shard, IndexReader indexReader) {
        beforeClose(shard,indexReader);
        try {
            indexReader.close();
        } catch (IOException e) {
            LOG.error("Error while closing index reader [{0}]",e,indexReader);
        }
        LOG.info("Index for table [{0}] shard [{1}] closed.",table,shard);
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
    
    private synchronized Map<String, IndexReader> openMissingShards(final String table, Set<String> shardsToServe, final Map<String, IndexReader> tableReaders) {
        Map<String, IndexReader> result = new HashMap<String, IndexReader>();
        Map<String,Future<Void>> futures = new HashMap<String, Future<Void>>();
        for (String s : shardsToServe) {
            final String shard = s;
            IndexReader indexReader = tableReaders.get(shard);
            if (indexReader == null) {
                futures.put(shard,executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        tableReaders.put(shard, openShard(table,shard));
                        return null;
                    }
                }));
            } else {
                result.put(shard, indexReader);
            }
        }
        for (String shard : futures.keySet()) {
            Future<Void> future = futures.get(shard);
            try {
                future.get();
                IndexReader indexReader = tableReaders.get(shard);
                result.put(shard, indexReader);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                LOG.error("Unknown error while opening shard [{0}] for table [{1}].",e.getCause(),shard,table);
                result.put(shard, EMPTY_INDEXREADER);
            }
        }
        return result;
    }

    private void setupReaders(String table) {
        readers.putIfAbsent(table, new ConcurrentHashMap<String, IndexReader>());
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
        List<String> offlineShardServers = getOfflineShardServers();
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
