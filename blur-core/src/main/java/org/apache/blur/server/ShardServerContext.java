package org.apache.blur.server;

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
import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.thirdparty.thrift_0_9_0.server.ServerContext;

/**
 * The thrift session that will hold index reader references to maintain across
 * query and fetch calls. Since there is a fixed size thread pool issuing calls
 * that involve the _threadsToContext map where Thread is the key we don't need
 * to clear or reset threads.
 */
public class ShardServerContext extends BlurServerContext implements ServerContext {

  private static final Log LOG = LogFactory.getLog(ShardServerContext.class);

  private final static Map<Thread, ShardServerContext> _threadsToContext = new ConcurrentHashMap<Thread, ShardServerContext>();
  private final Map<String, IndexSearcherCloseable> _indexSearcherMap = new ConcurrentHashMap<String, IndexSearcherCloseable>();

  public ShardServerContext(SocketAddress localSocketAddress, SocketAddress remoteSocketAddress) {
    super(localSocketAddress, remoteSocketAddress);
  }

  /**
   * Registers the {@link ShardServerContext} for this thread.
   * 
   * @param context
   *          the {@link ShardServerContext}.
   */
  public static void registerContextForCall(ShardServerContext context) {
    _threadsToContext.put(Thread.currentThread(), context);
  }

  /**
   * Gets the {@link ShardServerContext} for this {@link Thread}.
   * 
   * @return the {@link ShardServerContext}.
   */
  public static ShardServerContext getShardServerContext() {
    return _threadsToContext.get(Thread.currentThread());
  }

  /**
   * Resets the context, this closes and releases the index readers.
   */
  public static void resetSearchers() {
    ShardServerContext shardServerContext = getShardServerContext();
    if (shardServerContext != null) {
      shardServerContext.reset();
    }
  }

  /**
   * Closes this context, this happens when the client closes it's connect to
   * the server.
   */
  public void close() {
    reset();
  }

  /**
   * Resets the {@link ShardServerContext} by closing the searchers.
   */
  public void reset() {
    Collection<IndexSearcherCloseable> values = _indexSearcherMap.values();
    for (IndexSearcherCloseable indexSearcherClosable : values) {
      LOG.debug("Closing [{0}]", indexSearcherClosable);
      closeQuietly(indexSearcherClosable);
    }
    _indexSearcherMap.clear();
  }

  public static void closeQuietly(Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException e) {
      LOG.error("Closing [{0}]", closeable);
    }
  }

  /**
   * Gets the cached {@link IndexSearcherCloseable} (if any) for the given table
   * and shard.
   * 
   * @param table
   *          the stable name.
   * @param shard
   *          the shard name.
   * @return the {@link IndexSearcherCloseable} or null if not present.
   */
  public IndexSearcherCloseable getIndexSearcherClosable(String table, String shard) {
    IndexSearcherCloseable indexSearcherClosable = _indexSearcherMap.get(getKey(table, shard));
    if (indexSearcherClosable != null) {
      LOG.debug("Using cached searcher [{0}] for table [{1}] shard [{2}]", indexSearcherClosable, table, shard);
    }
    return indexSearcherClosable;
  }

  /**
   * Sets the index searcher for this {@link ShardServerContext} for the given
   * table and shard.
   * 
   * @param table
   *          the table name.
   * @param shard
   *          the shard name.
   * @param searcher
   *          the {@link IndexSearcherCloseable}.
   * @throws IOException
   */
  public void setIndexSearcherClosable(String table, String shard, IndexSearcherCloseable searcher) throws IOException {
    IndexSearcherCloseable indexSearcherClosable = _indexSearcherMap.put(getKey(table, shard), searcher);
    if (indexSearcherClosable != null && searcher != indexSearcherClosable) {
      indexSearcherClosable.close();
    }
  }

  private String getKey(String table, String shard) {
    return table + "/" + shard;
  }

}
