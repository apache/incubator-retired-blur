package org.apache.blur.console.util;

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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class CachingBlurClient {

  private final int timeout;
  private final Log log = LogFactory.getLog(CachingBlurClient.class);

  private Map<String, Item> clusterListCache = new HashMap<String, Item>(1);
  private Map<String, Item> tableListCache = new HashMap<String, Item>();
  private Map<String, Item> queryListCache = new HashMap<String, Item>();
  private Map<String, Item> queryStatusCache = new HashMap<String, Item>();
  private Map<String, Item> tableDescriptionCache = new HashMap<String, Item>();
  private Map<String, Item> tableStatsCache = new HashMap<String, Item>();
  private Map<String, Item> schemaCache = new HashMap<String, Item>();
  private Map<String, Item> controllerListCache = new HashMap<String, Item>();
  private Map<String, Item> shardListCache = new HashMap<String, Item>();

  private long cacheHits;
  private long cacheMisses;

  public CachingBlurClient(int timeout) {
    this.timeout = timeout;
    if(this.timeout > 0) {
      startCleanup();
    }
  }

  public List<String> shardClusterList() throws TException {
    return getFromCache(null, clusterListCache, new Retriever<List<String>>() {
      @Override
      public List<String> retrieve() throws TException {
        return getClient().shardClusterList();
      }
    });
  }

  private void startCleanup() {
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        boolean run = true;
          while (run) {
            try {
              cleanup(clusterListCache);
              cleanup(tableListCache);
              cleanup(queryListCache);
              cleanup(queryStatusCache);
              cleanup(tableDescriptionCache);
              cleanup(tableStatsCache);
              cleanup(schemaCache);
              cleanup(controllerListCache);
              cleanup(shardListCache);
              log.info("Cache: " + cacheHits + " hits, " + cacheMisses + " misses");
            } catch (Exception e) {
              log.error("Error cleaning up all caches", e);
            }
            try {
              Thread.sleep(timeout * 2);
            } catch (InterruptedException e) {
              run = false;
            }
          }

      }
    });
    t.setDaemon(true);
    t.start();
  }

  private void cleanup(Map<String, Item> cache) {
    if (cache != null) {
      try {
        synchronized (cache) {
          Iterator<Map.Entry<String, Item>> iterator = cache.entrySet().iterator();
          while (iterator.hasNext()) {
            Map.Entry<String, Item> entry = iterator.next();
            if (entry.getValue().expired(timeout)) {
              iterator.remove();
            }
          }
        }
      } catch (Exception e) {
      	log.error("Error cleaning up cache", e);
      }
    }
  }

  private Iface getClient() {
    return BlurClient.getClient(Config.getBlurConfig());
  }

  @SuppressWarnings("unchecked")
  private <T extends Object> T getFromCache(String key, Map<String, Item> cache, Retriever<T> retriever) throws TException {
    synchronized (cache) {
      if(this.timeout <= 0) {
        return retriever.retrieve();
      }
      Item item = cache.get(key);
      if (item == null || item.expired(timeout)) {
        cacheMisses++;
        item = new Item(retriever.retrieve());
        cache.put(key, item);
      } else {
        cacheHits++;
      }
      return (T) item.getValue();
    }
  }

  public void cancelQuery(String table, String uuid) throws TException {
    getClient().cancelQuery(table, uuid);
    invalidateQuery(table, uuid);
  }

  public void createTable(TableDescriptor td) throws TException {
    getClient().createTable(td);
    cleanup(tableListCache);
  }
  
  public void addColumnDefinition(String table, ColumnDefinition def) throws BlurException, TException {
	  getClient().addColumnDefinition(table, def);
  }

  private void invalidateQuery(String table, String uuid) {
    synchronized (queryStatusCache) {
      queryStatusCache.remove(queryKey(table,uuid));
    }
  }

  private void invalidateTable(String table) {
    synchronized (tableListCache) {
      tableListCache.clear();
    }
  }

  private String queryKey(String table, String uuid) {
    return table + ":" + uuid;
  }

  public List<String> enabledTables() throws TException {
    List<String> tables = tableList();
    List<String> enabledTables = new ArrayList<String>(tables.size());
    for (String table : tables) {
      try {
        TableDescriptor descriptor = describe(table);
        if (descriptor != null && descriptor.isEnabled()) {
          enabledTables.add(table);
        }
      } catch(TException e) {
        log.warn("error getting descriptor for " + table, e);
      }
    }
    return enabledTables;
  }

  public List<String> tableList() throws TException {
    return getFromCache(null, tableListCache, new Retriever<List<String>>() {
      @Override
      public List<String> retrieve() throws TException {
        return getClient().tableList();
      }
    });
  }
  
  public List<String> controllerList() throws TException {
	  return getFromCache(null, controllerListCache, new Retriever<List<String>>() {
  		@Override
  		public List<String> retrieve() throws TException {
  			return getClient().controllerServerList();
  		}
	  });
  }
  
  public List<String> shardList(final String cluster) throws TException {
    return getFromCache(cluster, shardListCache, new Retriever<List<String>>() {
      @Override
      public List<String> retrieve() throws TException {
        return getClient().shardServerList(cluster);
      }
    });
  }

  public List<String> queryStatusIdList(final String table) throws TException {
    return getFromCache(table, queryListCache, new Retriever<List<String>>() {
      @Override
      public List<String> retrieve() throws TException {
        return getClient().queryStatusIdList(table);
      }
    });
  }

  public BlurQueryStatus queryStatusById(final String table, final String id) throws TException {
    return getFromCache(queryKey(table,id), queryStatusCache, new Retriever<BlurQueryStatus>() {
      @Override
      public BlurQueryStatus retrieve() throws TException {
        return getClient().queryStatusById(table, id);
      }
    });

  }

  public List<String> tableListByCluster(final String cluster) throws TException {
    return getFromCache(cluster, tableListCache, new Retriever<List<String>>() {
      @Override
      public List<String> retrieve() throws TException {
        return getClient().tableListByCluster(cluster);
      }
    });
  }

  public TableDescriptor describe(final String table) throws TException {
    return getFromCache(table, tableDescriptionCache, new Retriever<TableDescriptor>() {
      @Override
      public TableDescriptor retrieve() throws TException {
        return getClient().describe(table);
      }
    });
  }

  public TableStats tableStats(final String table) throws TException {
    return getFromCache(table, tableStatsCache, new Retriever<TableStats>() {
      @Override
      public TableStats retrieve() throws TException {
        return getClient().tableStats(table);
      }
    });
  }

  public Schema schema(final String table) throws TException {
    return getFromCache(table, schemaCache, new Retriever<Schema>() {
      @Override
      public Schema retrieve() throws TException {
        return getClient().schema(table);
      }
    });
  }

  public List<String> terms(String table, String family, String column, String startWith, short i) throws TException {
    TableDescriptor descriptor = describe(table);
    if (descriptor != null && descriptor.isEnabled()) {
      return getClient().terms(table, family, column, startWith, i);
    }
    return new ArrayList<String>(0);
  }

  public void disableTable(String table) throws TException {
    getClient().disableTable(table);
    invalidateTable(table);
  }

  public void enableTable(String table) throws TException {
    getClient().enableTable(table);
    invalidateTable(table);
  }

  public void removeTable(String table, boolean includeFiles) throws TException {
    getClient().removeTable(table, includeFiles);
    invalidateTable(table);
  }

  private static class Item {
    private Object value;
    private long insertionTime;

    public Item(Object value) {
      this.value = value;
      insertionTime = System.currentTimeMillis();
    }

    public boolean expired(long timeout) {
      return insertionTime < System.currentTimeMillis() - timeout;
    }

    public Object getValue() {
      return value;
    }
  }

  private abstract static class Retriever<T> {
    public abstract T retrieve() throws TException;
  }
}
