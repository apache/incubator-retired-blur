package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.manager.IndexServer;

public abstract class DistributedIndexServer implements IndexServer {
    
    private ConcurrentHashMap<String,Map<String,IndexReader>> readers = new ConcurrentHashMap<String, Map<String,IndexReader>>();
    private String nodeName;
    //need a GC daemon for closing indexes
    //need a daemon for tracking what indexes to open
    //need a daemon to track reopening changed indexes

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

    private synchronized Map<String, IndexReader> openMissingShards(String table, Set<String> shardsToServe, Map<String, IndexReader> tableReaders) {
        Map<String, IndexReader> result = new HashMap<String, IndexReader>();
        for (String shard : shardsToServe) {
            IndexReader indexReader = tableReaders.get(shard);
            if (indexReader == null) {
                indexReader = openShard(table,shard);
                tableReaders.put(shard, indexReader);
            }
            result.put(shard, indexReader);
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

    protected abstract IndexReader openShard(String table, String shard);

    public String getNodeName() {
        return nodeName;
    }

    public DistributedIndexServer setNodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

}
