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

package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Similarity;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.manager.writer.BlurIndex;

public interface IndexServer {
    
    public enum TABLE_STATUS {
        ENABLED,
        DISABLED
    }
    
    /**
     * Gets the similarity object used by lucene for this table.
     * @param table the table name.
     * @return the similarity object.
     */
    Similarity getSimilarity(String table);
    
    /**
     * Gets the status of the table.
     * @param table the table name.
     * @return the status.
     */
    TABLE_STATUS getTableStatus(String table);
    
    /**
     * Gets the analyzer for the table.
     * @param table the table name.
     * @return the analyzer for lucene.
     */
    BlurAnalyzer getAnalyzer(String table);

    /**
     * Gets a map of the index readers for current running node.
     * <p/>
     * Keys are shard names, values are the associated indexes.
     *
     * @param table the table name.
     * @return the map of readers.
     * @throws IOException
     */
    Map<String, BlurIndex> getIndexes(String table) throws IOException;

    /**
     * The table name list.
     * @return the list of tables.
     */
    List<String> getTableList();
    
    /** 
     * The shard list for a given table.
     * @param table the table name.
     * @return the list of shards.
     */
    List<String> getShardList(String table);
    
    /**
     * Closes the index server.
     */
    void close();
    
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
    
    /**
     * Gets the current nodes name.
     * @return
     */
    String getNodeName();

}
