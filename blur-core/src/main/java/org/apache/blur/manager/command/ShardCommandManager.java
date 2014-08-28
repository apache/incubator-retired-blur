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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.command.primitive.DocumentCount;
import org.apache.blur.manager.command.primitive.DocumentCountAggregator;
import org.apache.blur.manager.command.primitive.PrimitiveCommand;
import org.apache.blur.manager.command.primitive.PrimitiveCommandAggregator;
import org.apache.blur.manager.command.primitive.ReadCommand;
import org.apache.blur.manager.command.primitive.ReadWriteCommand;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.IndexSearcherClosable;

public class ShardCommandManager implements Closeable {

  private final IndexServer _indexServer;
  private final ExecutorService _executorService;
  private final Map<String, PrimitiveCommand> _command = new ConcurrentHashMap<String, PrimitiveCommand>();

  public ShardCommandManager(IndexServer indexServer, int threadCount) throws IOException {
    register(DocumentCount.class);
    register(DocumentCountAggregator.class);
    _indexServer = indexServer;
    _executorService = Executors.newThreadPool("command-", threadCount);
  }

  private void register(Class<? extends PrimitiveCommand> commandClass) throws IOException {
    try {
      PrimitiveCommand command = commandClass.newInstance();
      _command.put(command.getName(), command);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  public Response execute(String table, String commandName, Args args) throws IOException {
    PrimitiveCommand command = getCommandObject(commandName);
    if (command == null) {
      throw new IOException("Command with name [" + commandName + "] not found.");
    }
    if (command instanceof ReadCommand) {
      return toResponse(executeReadCommand((ReadCommand<?>) command, table, args), command);
    } else if (command instanceof ReadWriteCommand) {
      return toResponse(executeReadWriteCommand((ReadWriteCommand<?>) command, table, args), command);
    }
    throw new IOException("Command type of [" + command.getClass() + "] not supported.");
  }

  @SuppressWarnings("unchecked")
  private Response toResponse(Map<String, Object> results, PrimitiveCommand command) throws IOException {
    if (command instanceof PrimitiveCommandAggregator) {
      PrimitiveCommandAggregator<Object, Object> primitiveCommandAggregator = (PrimitiveCommandAggregator<Object, Object>) command;
      Iterator<Entry<String, Object>> iterator = results.entrySet().iterator();
      Object object = primitiveCommandAggregator.aggregate(iterator);
      return Response.createNewAggregateResponse(object);
    }
    return Response.createNewResponse(results);
  }

  private Map<String, Object> executeReadWriteCommand(ReadWriteCommand<?> command, String table, Args args) {
    return null;
  }

  private Map<String, Object> executeReadCommand(ReadCommand<?> command, String table, final Args args)
      throws IOException {
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    Map<String, Future<?>> futureMap = new HashMap<String, Future<?>>();
    for (Entry<String, BlurIndex> e : indexes.entrySet()) {
      String shardId = e.getKey();
      final BlurIndex blurIndex = e.getValue();
      final ReadCommand<?> readCommand = command.clone();
      Future<Object> future = _executorService.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          IndexSearcherClosable searcher = blurIndex.getIndexSearcher();
          try {
            return readCommand.execute(args, searcher);
          } finally {
            searcher.close();
          }
        }
      });
      futureMap.put(shardId, future);
    }
    Map<String, Object> resultMap = new HashMap<String, Object>();
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
      resultMap.put(e.getKey(), object);
    }
    return resultMap;
  }

  private PrimitiveCommand getCommandObject(String commandName) {
    return _command.get(commandName);
  }

  @Override
  public void close() throws IOException {
    _executorService.shutdownNow();
  }

}
