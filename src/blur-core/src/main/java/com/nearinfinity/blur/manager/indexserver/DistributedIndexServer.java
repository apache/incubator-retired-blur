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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.manager.IndexServer;

public abstract class DistributedIndexServer implements IndexServer {
    
    private static final Log LOG = LogFactory.getLog(DistributedIndexServer.class);
    
    private ConcurrentHashMap<String,Map<String,IndexReader>> readers = new ConcurrentHashMap<String, Map<String,IndexReader>>();
    private String nodeName;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Timer readerCloserDaemon;
    private long delay = TimeUnit.MINUTES.toMillis(1);
    private Map<String,DistributedLayoutManager> layoutManagers = new ConcurrentHashMap<String, DistributedLayoutManager>();
    private Map<String, Set<String>> layoutCache = new ConcurrentHashMap<String, Set<String>>();
    //need a GC daemon for closing indexes
    //need a daemon for tracking what indexes to open ???
    //need a daemon to track reopening changed indexes
    
    public DistributedIndexServer init() {
        startIndexReaderCloserDaemon();
        startIndexReopenerDaemon();
        return this;
    }
    
    protected abstract IndexReader openShard(String table, String shard);
    
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
            beforeClose(shard,indexReader);
            try {
                indexReader.close();
            } catch (IOException e) {
                LOG.error("Error while closing index reader [" + indexReader + "]",e);
            }
        }
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }
    
    //Getters and setters
    
    public String getNodeName() {
        return nodeName;
    }

    public DistributedIndexServer setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    public long getDelay() {
        return delay;
    }

    public DistributedIndexServer setDelay(long delay) {
        this.delay = delay;
        return this;
    }
    
    //Getters and setters
    
    private synchronized Map<String, IndexReader> openMissingShards(final String table, Set<String> shardsToServe, final Map<String, IndexReader> tableReaders) {
        Map<String, IndexReader> result = new HashMap<String, IndexReader>();
        List<Future<String>> futures = new ArrayList<Future<String>>();
        for (String s : shardsToServe) {
            final String shard = s;
            IndexReader indexReader = tableReaders.get(shard);
            if (indexReader == null) {
                futures.add(executorService.submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        tableReaders.put(shard, openShard(table,shard));
                        return shard;
                    }
                }));
            } else {
                result.put(shard, indexReader);
            }
        }
        for (Future<String> future : futures) {
            try {
                String shard = future.get();
                IndexReader indexReader = tableReaders.get(shard);
                result.put(shard, indexReader);
            } catch (Exception e) {
                throw new RuntimeException(e);
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
        layoutManager.setNodes(getShardServerList());
        layoutManager.setNodesOffline(getOfflineShardServers());
        layoutManager.setShards(getShardList(table));
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
    
    private void startIndexReopenerDaemon() {
        
    }

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
