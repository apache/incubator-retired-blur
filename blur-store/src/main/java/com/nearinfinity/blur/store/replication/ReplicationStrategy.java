package com.nearinfinity.blur.store.replication;

public interface ReplicationStrategy {
    
    boolean replicateLocally(String table, String name);

}
