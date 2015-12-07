package org.apache.blur.manager.indexserver;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.ShardState;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * This class holds the current state of any given shard within the shard
 * server.
 * 
 */
public class ShardStateManager implements Closeable {

  private static final Log LOG = LogFactory.getLog(ShardStateManager.class);

  private static final long _5_SECONDS = TimeUnit.SECONDS.toMillis(5);
  private static final long _60_SECONDS = TimeUnit.SECONDS.toMillis(60);
  private final Map<Key, Value> stateMap = new ConcurrentHashMap<Key, Value>();
  private final Timer timer;

  public ShardStateManager() {
    timer = new Timer("ShardStateManager-cleanup", true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          cleanup();
        } catch (Throwable t) {
          LOG.error("Unkown error whiel trying to cleanup shard state manager.", t);
        }
      }

      private void cleanup() {
        Collection<Key> toBeDeleted = null;
        for (Entry<Key, Value> e : stateMap.entrySet()) {
          if (shouldBeRemoved(e)) {
            if (toBeDeleted == null) {
              toBeDeleted = new HashSet<ShardStateManager.Key>();
            }
            toBeDeleted.add(e.getKey());
          }
        }
        if (toBeDeleted != null) {
          for (Key k : toBeDeleted) {
            stateMap.remove(k);
          }
        }
      }

      private boolean shouldBeRemoved(Entry<Key, Value> e) {
        if (e.getValue().timeToBeRemoved < System.currentTimeMillis()) {
          return true;
        }
        return false;
      }
    }, _5_SECONDS, _5_SECONDS);
  }

  public void opening(String table, String shard) {
    setState(table, shard, ShardState.OPENING);
  }

  public void open(String table, String shard) {
    setState(table, shard, ShardState.OPEN);
  }

  public void openingError(String table, String shard) {
    setState(table, shard, ShardState.OPENING_ERROR);
  }

  public void closing(String table, String shard) {
    setState(table, shard, ShardState.CLOSING);
  }

  public void closed(String table, String shard) {
    setState(table, shard, ShardState.CLOSED);
  }

  public void closingError(String table, String shard) {
    setState(table, shard, ShardState.CLOSING_ERROR);
  }

  public Map<String, ShardState> getShardState(String table) {
    Map<String, ShardState> result = new HashMap<String, ShardState>();
    List<Entry<Key, Value>> entryList = new ArrayList<Entry<Key, Value>>(stateMap.entrySet());
    for (Entry<Key, Value> entry : entryList) {
      Key key = entry.getKey();
      if (key.table.equals(table)) {
        result.put(key.shard, entry.getValue().shardState);
      }
    }
    return result;
  }

  private void setState(String table, String shard, ShardState state) {
    switch (state) {
    case CLOSED:
    case CLOSING_ERROR:
      stateMap.put(new Key(table, shard), new Value(state, System.currentTimeMillis() + _60_SECONDS));
      return;
    default:
      stateMap.put(new Key(table, shard), new Value(state));
      return;
    }
  }

  private static class Value {
    final ShardState shardState;
    final long timeToBeRemoved;

    Value(ShardState shardState) {
      this(shardState, Long.MAX_VALUE);
    }

    Value(ShardState shardState, long timeToBeRemoved) {
      this.shardState = shardState;
      this.timeToBeRemoved = timeToBeRemoved;
    }
  }

  private static class Key {
    final String table;
    final String shard;

    Key(String table, String shard) {
      this.table = table;
      this.shard = shard;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((shard == null) ? 0 : shard.hashCode());
      result = prime * result + ((table == null) ? 0 : table.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (shard == null) {
        if (other.shard != null)
          return false;
      } else if (!shard.equals(other.shard))
        return false;
      if (table == null) {
        if (other.table != null)
          return false;
      } else if (!table.equals(other.table))
        return false;
      return true;
    }
  }

  @Override
  public void close() {
    timer.cancel();
    timer.purge();
  }

}
