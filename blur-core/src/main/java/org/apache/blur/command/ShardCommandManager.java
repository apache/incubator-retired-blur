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
package org.apache.blur.command;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.ShardServerContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.IndexReader;

public class ShardCommandManager extends BaseCommandManager {

  private final IndexServer _indexServer;

  public ShardCommandManager(IndexServer indexServer, File tmpPath, String commandPath, int workerThreadCount,
      int driverThreadCount, long connectionTimeout, Configuration configuration, String serverName) throws IOException {
    super(tmpPath, commandPath, workerThreadCount, driverThreadCount, connectionTimeout, configuration, serverName);
    _indexServer = indexServer;
  }

  public Response execute(final TableContextFactory tableContextFactory, final String commandName,
      final ArgumentOverlay argumentOverlay, CommandStatus originalCommandStatusObject) throws IOException,
      TimeoutException, ExceptionCollector {
    final ShardServerContext shardServerContext = getShardServerContext();
    AtomicBoolean running = new AtomicBoolean(true);
    final Command<?> command = getCommandObject(commandName, argumentOverlay);
    Callable<Response> callable = new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        if (command == null) {
          throw new IOException("Command with name [" + commandName + "] not found.");
        }
        if (command instanceof IndexRead || command instanceof ServerRead) {
          return toResponse(
              executeReadCommand(shardServerContext, command, tableContextFactory, originalCommandStatusObject, running),
              command, getServerContext(tableContextFactory));
        }
        throw new IOException("Command type of [" + command.getClass() + "] not supported.");
      }

      private CombiningContext getServerContext(final TableContextFactory tableContextFactory) {
        return new CombiningContext() {

          @Override
          public TableContext getTableContext(String table) throws IOException {
            return tableContextFactory.getTableContext(table);
          }

          @Override
          public BlurConfiguration getBlurConfiguration(String table) throws IOException {
            return getTableContext(table).getBlurConfiguration();
          }
        };
      }
    };
    return submitDriverCallable(callable, command, originalCommandStatusObject, running);
  }

  private ShardServerContext getShardServerContext() {
    ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
    if (shardServerContext == null) {
      shardServerContext = new ShardServerContext(null, null);
    }
    return shardServerContext;
  }

  @SuppressWarnings("unchecked")
  private Response toResponse(Map<Shard, Object> results, Command<?> command, CombiningContext serverContext)
      throws IOException, InterruptedException {
    if (command instanceof ServerRead) {
      ServerRead<Object, Object> primitiveCommandAggregator = (ServerRead<Object, Object>) command;
      Object object = primitiveCommandAggregator.combine(serverContext, results);
      return Response.createNewAggregateResponse(object);
    }
    return Response.createNewShardResponse(results);
  }

  private Map<Shard, Object> executeReadCommand(ShardServerContext shardServerContext, Command<?> command,
      final TableContextFactory tableContextFactory, CommandStatus originalCommandStatusObject, AtomicBoolean running)
      throws IOException, ExceptionCollector {
    BaseContext context = new BaseContext() {
      @Override
      public TableContext getTableContext(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public BlurConfiguration getBlurConfiguration(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
    Set<String> tables = command.routeTables(context);
    if (tables.isEmpty()) {
      throw new IOException("At least one table needs to specified.");
    }
    Map<String, Set<Shard>> shardMap = toMap(command.routeShards(context, tables));

    Map<Shard, Future<?>> futureMap = new HashMap<Shard, Future<?>>();
    for (String table : tables) {
      Set<Shard> shardSet = shardMap.get(table);
      boolean checkShards = shardSet == null ? false : !shardSet.isEmpty();

      Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
      for (Entry<String, BlurIndex> e : indexes.entrySet()) {
        String shardId = e.getKey();
        final Shard shard = new Shard(table, shardId);
        if (checkShards) {
          if (!shardSet.contains(shard)) {
            continue;
          }
        }
        final BlurIndex blurIndex = e.getValue();
        Callable<Object> callable;
        Command<?> clone = command.clone();
        if (clone instanceof IndexRead) {
          final IndexRead<?> readCommand = (IndexRead<?>) clone;
          callable = getCallable(shardServerContext, tableContextFactory, table, shard, blurIndex, readCommand, running);
        } else if (clone instanceof ServerRead) {
          final ServerRead<?, ?> readCombiningCommand = (ServerRead<?, ?>) clone;
          callable = getCallable(shardServerContext, tableContextFactory, table, shard, blurIndex,
              readCombiningCommand, running);
        } else {
          throw new IOException("Command type of [" + clone.getClass() + "] not supported.");
        }
        Future<Object> future = submitToExecutorService(callable, clone, originalCommandStatusObject, running);
        futureMap.put(shard, future);
      }
    }
    Map<Shard, Object> resultMap = new HashMap<Shard, Object>();
    ExceptionCollector collector = null;
    for (Entry<Shard, Future<?>> e : futureMap.entrySet()) {
      Shard shard = e.getKey();
      Future<?> future = e.getValue();
      Object object;
      try {
        object = future.get();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } catch (ExecutionException ex) {
        if (collector == null) {
          collector = new ExceptionCollector();
        }
        collector.add(ex.getCause());
        continue;
      }
      resultMap.put(shard, object);
    }
    if (collector != null) {
      throw collector;
    }
    return resultMap;
  }

  private Map<String, Set<Shard>> toMap(Set<Shard> shards) {
    Map<String, Set<Shard>> result = new HashMap<String, Set<Shard>>();
    for (Shard shard : shards) {
      Set<Shard> shardSet = result.get(shard.getTable());
      if (shardSet == null) {
        result.put(shard.getTable(), shardSet = new TreeSet<Shard>());
      }
      shardSet.add(shard);
    }
    return result;
  }

  private Callable<Object> getCallable(final ShardServerContext shardServerContext,
      final TableContextFactory tableContextFactory, final String table, final Shard shard, final BlurIndex blurIndex,
      final ServerRead<?, ?> readCombiningCommand, AtomicBoolean running) {
    return new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        String shardId = shard.getShard();
        IndexSearcherCloseable searcher = shardServerContext.getIndexSearcherClosable(table, shardId);
        if (searcher == null) {
          searcher = blurIndex.getIndexSearcher();
          shardServerContext.setIndexSearcherClosable(table, shardId, searcher);
        }
        return readCombiningCommand
            .execute(new ShardIndexContext(tableContextFactory, table, shard, searcher, running));
      }
    };
  }

  private Callable<Object> getCallable(final ShardServerContext shardServerContext,
      final TableContextFactory tableContextFactory, final String table, final Shard shard, final BlurIndex blurIndex,
      final IndexRead<?> readCommand, AtomicBoolean running) {
    return new Callable<Object>() {
      @Override
      public Object call() throws Exception {

        String shardId = shard.getShard();
        IndexSearcherCloseable searcher = shardServerContext.getIndexSearcherClosable(table, shardId);
        if (searcher == null) {
          searcher = blurIndex.getIndexSearcher();
          shardServerContext.setIndexSearcherClosable(table, shardId, searcher);
        }
        return readCommand.execute(new ShardIndexContext(tableContextFactory, table, shard, searcher, running));
      }
    };
  }

  static class ShardIndexContext extends IndexContext {

    private final Shard _shard;
    private final IndexSearcherCloseable _searcher;
    private final TableContextFactory _tableContextFactory;
    private final String _table;

    public ShardIndexContext(TableContextFactory tableContextFactory, String table, Shard shard,
        IndexSearcherCloseable searcher, AtomicBoolean running) {
      _tableContextFactory = tableContextFactory;
      _table = table;
      _shard = shard;
      _searcher = searcher;
      IndexManager.resetExitableReader(getIndexReader(), running);
    }

    @Override
    public IndexReader getIndexReader() {
      return getIndexSearcher().getIndexReader();
    }

    @Override
    public IndexSearcherCloseable getIndexSearcher() {
      return _searcher;
    }

    @Override
    public TableContext getTableContext() throws IOException {
      return _tableContextFactory.getTableContext(_table);
    }

    @Override
    public Shard getShard() {
      return _shard;
    }

    @Override
    public BlurConfiguration getBlurConfiguration() throws IOException {
      return getTableContext().getBlurConfiguration();
    }

    @Override
    public BlurConfiguration getBlurConfiguration(String table) throws IOException {
      return getTableContext(table).getBlurConfiguration();
    }

    @Override
    public TableContext getTableContext(String table) throws IOException {
      return _tableContextFactory.getTableContext(table);
    }

  }

}
