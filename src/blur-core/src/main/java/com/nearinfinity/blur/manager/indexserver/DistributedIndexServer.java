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

    public String getNodeName() {
        return nodeName;
    }

    public DistributedIndexServer setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }
    
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
        TreeSet<String> shards = new TreeSet<String>();
        List<String> shardList = getShardList(table);
        List<String> shardServerList = getShardServerList();
        int index = getNodeIndex(table,shardServerList);
        int serverCount = shardServerList.size();
        int size = shardList.size();
        for (int i = 0; i < size; i++) {
            if (i % serverCount == index) {
                shards.add(shardList.get(i));
            }
        }
        return shards;
    }

    private int getNodeIndex(String table, List<String> shardServerList) {
        int index = shardServerList.indexOf(getNodeName());
        if (index == -1) {
            throw new RuntimeException("Node [" + getNodeName() +
                    "] is not currently online.");
        }
        int size = shardServerList.size();
        int offset = Math.abs(table.hashCode() % size);
        index += offset;
        if (index >= size) {
            index = index - size;
        }
        return index;
    }
    
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

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    public long getDelay() {
        return delay;
    }

    public DistributedIndexServer setDelay(long delay) {
        this.delay = delay;
        return this;
    }
}
