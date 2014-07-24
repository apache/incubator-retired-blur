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
package org.apache.blur.server.platform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.blur.manager.writer.BlurIndex;

public abstract class Command<T1, T2> {

  private ExecutorService _executorService;
  private Object[] _args;

  public Object[] getArgs() {
    return _args;
  }

  public void setArgs(Object[] args) {
    _args = args;
  }

  public void setExecutorService(ExecutorService executorService) {
    _executorService = executorService;
  }

  public T2 process(Map<String, Map<String, BlurIndex>> indexes) throws CommandException, IOException {
    List<Future<Map<TableShardKey, T1>>> futures = new ArrayList<Future<Map<TableShardKey, T1>>>();
    for (Entry<String, Map<String, BlurIndex>> tableEntry : indexes.entrySet()) {
      String table = tableEntry.getKey();
      Map<String, BlurIndex> shards = tableEntry.getValue();
      for (Entry<String, BlurIndex> shardEntry : shards.entrySet()) {
        String shard = shardEntry.getKey();
        final BlurIndex blurIndex = shardEntry.getValue();
        final TableShardKey tableShardKey = new TableShardKey(table, shard);
        futures.add(_executorService.submit(new Callable<Map<TableShardKey, T1>>() {
          @Override
          public Map<TableShardKey, T1> call() throws Exception {
            return processShard(tableShardKey, blurIndex);
          }
        }));
      }
    }

    CommandException commandException = new CommandException();
    boolean error = false;
    Map<TableShardKey, T1> results = new HashMap<TableShardKey, T1>();
    for (Future<Map<TableShardKey, T1>> future : futures) {
      try {
        results.putAll(future.get());
      } catch (InterruptedException e) {
        commandException.addSuppressed(e);
        error = true;
      } catch (ExecutionException e) {
        commandException.addSuppressed(e.getCause());
        error = true;
      }
    }
    if (error) {
      throw commandException;
    }
    return merge(results);

  }

  public abstract T2 merge(Map<TableShardKey, T1> results) throws IOException;

  public abstract Map<TableShardKey, T1> processShard(TableShardKey tableShardKey, BlurIndex blurIndex)
      throws IOException;

}
