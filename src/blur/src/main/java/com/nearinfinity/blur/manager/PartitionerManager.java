package com.nearinfinity.blur.manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.nearinfinity.mele.Mele;

public class PartitionerManager {
    
    private Map<String, Partitioner> partitioners = new ConcurrentHashMap<String, Partitioner>();
    private Mele mele;

    public PartitionerManager(Mele mele) {
        this.mele = mele;
        
    }

    public synchronized Partitioner getPartitioner(String table) {
        Partitioner partitioner = partitioners.get(table);
        if (partitioner == null) {
            return checkMele(table);
        }
        return partitioner;
    }

    private Partitioner checkMele(String table) {
        List<String> listDirectories = mele.listDirectories(table);
        Partitioner partitioner = new Partitioner(listDirectories);
        partitioners.put(table, partitioner);
        return partitioner;
    }

}
