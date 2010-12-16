package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

public class LayoutManager {
    
    private static final SortedSet<String> EMPTY_SORTED_SET = new TreeSet<String>();
    
    private SortedSet<String> nodes = EMPTY_SORTED_SET;
    private Set<String> nodesOffline = EMPTY_SORTED_SET;
    private SortedSet<String> shards = EMPTY_SORTED_SET;
    private List<String> nodeList = new ArrayList<String>();
    private Map<String, String> cache = new TreeMap<String, String>();

    public LayoutManager init() {
        if (nodesOffline.equals(nodes)) {
            cache = getLockedMap(new TreeMap<String, String>());
            return this;
        }
        Map<String, String> mappings = new TreeMap<String, String>();
        SortedSet<String> moveBecauseOfDownNodes = new TreeSet<String>();
        int nodeListSize = nodeList.size();
        int nodeCount = 0;
        for (String shard : shards) {
            String node = nodeList.get(nodeCount);
            mappings.put(shard, node);
            if (nodesOffline.contains(node)) {
                moveBecauseOfDownNodes.add(shard);
            }
            nodeCount++;
            if (nodeCount >= nodeListSize) {
                nodeCount = 0;
            }
        }
        for (String shard : moveBecauseOfDownNodes) {
            String node = nodeList.get(nodeCount);
            while (isOffline(node)) {
                nodeCount++;
                if (nodeCount >= nodeListSize) {
                    nodeCount = 0;
                }
                node = nodeList.get(nodeCount);
            }
            mappings.put(shard, node);
            nodeCount++;
            if (nodeCount >= nodeListSize) {
                nodeCount = 0;
            }
        }
        cache = getLockedMap(mappings);
        return this;
    }

    public Map<String, String> getLayout() {
        return cache;
    }

    private boolean isOffline(String node) {
        return nodesOffline.contains(node);
    }

    public Collection<String> getNodes() {
        return new TreeSet<String>(nodes);
    }

    public LayoutManager setNodes(Collection<String> v) {
        this.nodes = new TreeSet<String>(v);
        this.nodeList = new ArrayList<String>(nodes);
        return this;
    }

    public Collection<String> getShards() {
        return new TreeSet<String>(shards);
    }

    public LayoutManager setShards(Collection<String> v) {
        this.shards = new TreeSet<String>(v);
        return this;
    }

    public Collection<String> getNodesOffline() {
        return new TreeSet<String>(nodesOffline);
    }

    public LayoutManager setNodesOffline(Collection<String> v) {
        this.nodesOffline = new HashSet<String>(v);
        return this;
    }
    
    private Map<String, String> getLockedMap(final Map<String, String> map) {
        final Set<Entry<String, String>> entrySet = wrapEntries(map.entrySet());
        return new Map<String,String>() {

            @Override
            public boolean containsKey(Object key) {
                return map.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
                return map.containsValue(value);
            }

            @Override
            public Set<java.util.Map.Entry<String, String>> entrySet() {
                return entrySet;
            }

            @Override
            public String get(Object key) {
                return map.get(key);
            }

            @Override
            public boolean isEmpty() {
                return map.isEmpty();
            }

            @Override
            public Set<String> keySet() {
                return new TreeSet<String>(map.keySet());
            }

            @Override
            public int size() {
                return map.size();
            }

            @Override
            public Collection<String> values() {
                return new TreeSet<String>(map.values());
            }
            
            @Override
            public void clear() {
                throw new RuntimeException("read only");
            }
            
            @Override
            public String put(String key, String value) {
                throw new RuntimeException("read only");
            }

            @Override
            public void putAll(Map<? extends String, ? extends String> m) {
                throw new RuntimeException("read only");
            }

            @Override
            public String remove(Object key) {
                throw new RuntimeException("read only");
            }

            @Override
            public String toString() {
                return map.toString();
            }
            
            @Override
            public boolean equals(Object obj) {
                return map.equals(obj);
            }

            @Override
            public int hashCode() {
                return map.hashCode();
            }
        };
    }

    private Set<Entry<String, String>> wrapEntries(Set<Entry<String, String>> entrySet) {
        Set<Entry<String, String>> result = new HashSet<Entry<String,String>>();
        for (Entry<String, String> entry : entrySet) {
            result.add(wrapEntry(entry));
        }
        return result;
    }

    private Entry<String, String> wrapEntry(final Entry<String, String> entry) {
        return new Entry<String, String>() {
            
            @Override
            public String setValue(String value) {
                throw new RuntimeException("read only");
            }
            
            @Override
            public String getValue() {
                return entry.getValue();
            }
            
            @Override
            public String getKey() {
                return entry.getKey();
            }

            @Override
            public String toString() {
                return entry.toString();
            }

            @Override
            public boolean equals(Object obj) {
                return entry.equals(obj);
            }

            @Override
            public int hashCode() {
                return entry.hashCode();
            }
        };
    }

}
