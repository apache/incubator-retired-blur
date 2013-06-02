package org.apache.blur.manager;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.lucene.search.similarities.Similarity;

/**
 * The {@link IndexServer} interface provides the internal API to interact with
 * the indexes being served in the shard server instance.
 */
public interface IndexServer {

  /**
   * Enum that describes whether a table is enabled or not.
   */
  public enum TABLE_STATUS {
    ENABLED, DISABLED
  }

  // Server state

  /**
   * Gets a sorted list of shards being served by this server.
   * 
   * @param table
   *          the table name
   * @return the sorted list of shards.
   */
  SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException;

  /**
   * Gets a map of the index readers for current running node.
   * <p/>
   * Keys are shard names, values are the associated indexes.
   * 
   * @param table
   *          the table name.
   * @return the map of readers.
   * @throws IOException
   */
  Map<String, BlurIndex> getIndexes(String table) throws IOException;

  // Table Meta Data

  /**
   * The shard list for a given table.
   * 
   * @param table
   *          the table name.
   * @return the list of shards.
   */
  List<String> getShardList(String table);

  /**
   * Gets the similarity object used by lucene for this table.
   * 
   * @param table
   *          the table name.
   * @return the similarity object.
   */
  Similarity getSimilarity(String table);

  /**
   * Gets the status of the table.
   * 
   * @param table
   *          the table name.
   * @return the status.
   */
  TABLE_STATUS getTableStatus(String table);

  /**
   * Gets the analyzer for the table.
   * 
   * @param table
   *          the table name.
   * @return the analyzer for lucene.
   */
  BlurAnalyzer getAnalyzer(String table);

  /**
   * Gets the current nodes name.
   * 
   * @return
   */
  String getNodeName();

  /**
   * Gets the table uri. (hdfs://cluster1:9000/blur/tables/tablename1234)
   * 
   * @param table
   *          the table name
   * @return the uri to the table directory that contains all the shards..
   */
  String getTableUri(String table);

  /**
   * Gets the shard count for the given table.
   * 
   * @param table
   *          the name of the table.
   * @return
   */
  int getShardCount(String table);

  // Metrics

  /**
   * Gets the record count of the table.
   * 
   * @param table
   *          the name of the table.
   * @return the record count.
   * @throws IOException
   */
  long getRecordCount(String table) throws IOException;

  /**
   * Gets the row count of the table.
   * 
   * @param table
   *          the name of the table.
   * @return
   * @throws IOException
   */
  long getRowCount(String table) throws IOException;

  /**
   * Gets the current on disk table size.
   * 
   * @param table
   *          the name of the table.
   * @return the number of bytes on disk.
   * @throws IOException
   */
  long getTableSize(String table) throws IOException;

  /**
   * Closes the index server.
   */
  void close();

  /**
   * Get the shard state. Provides access to the as is state of the shards in
   * this instance.
   * 
   * @param table
   *          the table name.
   * @return the map of shard name to state.
   */
  Map<String, ShardState> getShardState(String table);

}
