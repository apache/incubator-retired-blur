package com.nearinfinity.blur.manager;

import java.util.List;

public interface ClusterInfo {
    
    /**
     * Gets a list of all the controller nodes in the cluster.
     * @return the controller node list.
     */
    List<String> getControllerServerList();
    
    /**
     * Gets a list of all the shard servers in the cluster up or down.
     * @return the shard node list.
     */
    List<String> getShardServerList();
    
    /**
     * Gets a list of all the shard servers that are currently offline.
     * NOTE: The node listed here are also in the shard server list.
     * @return the offline shards servers.
     */
    List<String> getOfflineShardServers();
    
    /**
     * Gets a list of all the shard servers that are currently offline.
     * NOTE: The node listed here are also in the shard server list.
     * @return the offline shards servers.
     */
    List<String> getOnlineShardServers();

}
