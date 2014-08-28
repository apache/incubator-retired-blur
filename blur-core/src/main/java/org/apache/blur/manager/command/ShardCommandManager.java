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
package org.apache.blur.manager.command;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.command.primitive.BaseCommand;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.TableContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

public class ShardCommandManager extends BaseCommandManager {

  private final IndexServer _indexServer;

  public ShardCommandManager(IndexServer indexServer, int threadCount) throws IOException {
    super(threadCount);
    _indexServer = indexServer;
  }

  public Response execute(TableContext tableContext, String commandName, Args args) throws IOException {
    BaseCommand command = getCommandObject(commandName);
    if (command == null) {
      throw new IOException("Command with name [" + commandName + "] not found.");
    }
    if (command instanceof IndexReadCommand) {
      return toResponse(executeReadCommand(command, tableContext, args), command);
    } else if (command instanceof IndexWriteCommand) {
      return toResponse(executeReadWriteCommand((IndexWriteCommand<?>) command, tableContext, args), command);
    }
    throw new IOException("Command type of [" + command.getClass() + "] not supported.");
  }

  @SuppressWarnings("unchecked")
  private Response toResponse(Map<Shard, Object> results, BaseCommand command) throws IOException {
    if (command instanceof IndexReadCombiningCommand) {
      IndexReadCombiningCommand<Object, Object> primitiveCommandAggregator = (IndexReadCombiningCommand<Object, Object>) command;
      Object object = primitiveCommandAggregator.combine(results);
      return Response.createNewAggregateResponse(object);
    }
    return Response.createNewShardResponse(results);
  }

  private Map<Shard, Object> executeReadWriteCommand(IndexWriteCommand<?> command, TableContext tableContext, Args args) {
    return null;
  }

  private Map<Shard, Object> executeReadCommand(BaseCommand command, final TableContext tableContext, final Args args)
      throws IOException {
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(tableContext.getTable());
    Map<String, Future<?>> futureMap = new HashMap<String, Future<?>>();
    for (Entry<String, BlurIndex> e : indexes.entrySet()) {
      String shardId = e.getKey();
      final Shard shard = new Shard(shardId);
      final BlurIndex blurIndex = e.getValue();
      final IndexReadCommand<?> readCommand = (IndexReadCommand<?>) command.clone();
      Future<Object> future = _executorService.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          IndexSearcherClosable searcher = blurIndex.getIndexSearcher();
          try {
            return readCommand.execute(new ShardIndexContext(tableContext, shard, searcher, args));
          } finally {
            searcher.close();
          }
        }
      });
      futureMap.put(shardId, future);
    }
    Map<Shard, Object> resultMap = new HashMap<Shard, Object>();
    for (Entry<String, Future<?>> e : futureMap.entrySet()) {
      Future<?> future = e.getValue();
      Object object;
      try {
        object = future.get();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } catch (ExecutionException ex) {
        throw new IOException(ex.getCause());
      }
      resultMap.put(new Shard(e.getKey()), object);
    }
    return resultMap;
  }

  static class ShardIndexContext extends IndexContext {

    private final TableContext _tableContext;
    private final Shard _shard;
    private final IndexSearcher _searcher;
    private final Args _args;

    public ShardIndexContext(TableContext tableContext, Shard shard, IndexSearcher searcher, Args args) {
      _tableContext = tableContext;
      _shard = shard;
      _searcher = searcher;
      _args = args;
    }

    @Override
    public Args getArgs() {
      return _args;
    }

    @Override
    public IndexReader getIndexReader() {
      return getIndexSearcher().getIndexReader();
    }

    @Override
    public IndexSearcher getIndexSearcher() {
      return _searcher;
    }

    @Override
    public TableContext getTableContext() {
      return _tableContext;
    }

    @Override
    public Shard getShard() {
      return _shard;
    }

  }
}
