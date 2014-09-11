package org.apache.blur.command;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.server.LayoutFactory;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.ClientPool;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.thrift.generated.ValueObject;
import org.apache.hadoop.conf.Configuration;

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

public class ControllerClusterContext extends ClusterContext implements Closeable {

  private final static Log LOG = LogFactory.getLog(ControllerClusterContext.class);

  private final Args _args;
  private final TableContextFactory _tableContextFactory;
  private final Map<Server, Client> _clientMap;
  private final ControllerCommandManager _manager;
  private final LayoutFactory _layoutFactory;

  public ControllerClusterContext(TableContextFactory tableContextFactory, LayoutFactory layoutFactory, Args args,
      ControllerCommandManager manager) throws IOException {
    _tableContextFactory = tableContextFactory;
    _args = args;
    _clientMap = getBlurClientsForCluster(layoutFactory.getServerConnections());
    _manager = manager;
    _layoutFactory = layoutFactory;
  }

  public Map<Server, Client> getBlurClientsForCluster(Set<Connection> serverConnections) throws IOException {
    Map<Server, Client> clients = new HashMap<Server, Client>();
    for (Connection serverConnection : serverConnections) {
      try {
        Client client = BlurClientManager.getClientPool().getClient(serverConnection);
        client.refresh();
        clients.put(new Server(serverConnection.getHost() + ":" + serverConnection.getPort()), client);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
    return clients;
  }

  @Override
  public Args getArgs() {
    return _args;
  }

  @Override
  public TableContext getTableContext(String table) throws IOException {
    return _tableContextFactory.getTableContext(table);
  }

  @Override
  public <T> Map<Shard, T> readIndexes(Args args, Class<? extends IndexReadCommand<T>> clazz) throws IOException {
    Map<Shard, Future<T>> futures = readIndexesAsync(args, clazz);
    Map<Shard, T> result = new HashMap<Shard, T>();
    return processFutures(clazz, futures, result);
  }

  @Override
  public <T> Map<Server, T> readServers(Args args, Class<? extends IndexReadCombiningCommand<?, T>> clazz)
      throws IOException {
    Map<Server, Future<T>> futures = readServersAsync(args, clazz);
    Map<Server, T> result = new HashMap<Server, T>();
    return processFutures(clazz, futures, result);
  }

  @Override
  public void close() throws IOException {
    ClientPool clientPool = BlurClientManager.getClientPool();
    for (Entry<Server, Client> e : _clientMap.entrySet()) {
      clientPool.returnClient(new Connection(e.getKey().getServer()), e.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Map<Shard, Future<T>> readIndexesAsync(final Args args, Class<? extends IndexReadCommand<T>> clazz)
      throws IOException {
    final String commandName = _manager.getCommandName((Class<? extends Command>) clazz);
    Command command = _manager.getCommandObject(commandName);
    Map<Shard, Future<T>> futureMap = new HashMap<Shard, Future<T>>();
    Set<String> tables = _manager.getTables(command, args);
    Map<String, Set<Shard>> shards = _manager.getShards(command, args, tables);
    Map<Server, Client> clientMap = getClientMap(command, args, tables, shards);

    for (Entry<Server, Client> e : clientMap.entrySet()) {
      Server server = e.getKey();
      final Client client = e.getValue();
      Future<Map<Shard, T>> future = _manager.submitToExecutorService(new Callable<Map<Shard, T>>() {
        @Override
        public Map<Shard, T> call() throws Exception {
          Arguments arguments = CommandUtil.toArguments(args);
          Response response = waitForResponse(client, commandName, arguments);
          Map<Shard, Object> shardToValue = CommandUtil.fromThriftToObject(response.getShardToValue());
          return (Map<Shard, T>) shardToValue;
        }
      });
      for (Shard shard : getShardsOnServer(server, tables, shards)) {
        futureMap.put(shard, new ShardResultFuture<T>(shard, future));
      }
    }
    return futureMap;
  }

  private Map<Server, Client> getClientMap(Command command, Args args, Set<String> tables,
      Map<String, Set<Shard>> shards) throws IOException {
    Map<Server, Client> result = new HashMap<Server, Client>();

    for (Entry<Server, Client> e : _clientMap.entrySet()) {
      Server server = e.getKey();
      if (_layoutFactory.isValidServer(server, tables, shards)) {
        result.put(server, e.getValue());
      }
    }
    return result;
  }

  protected static Response waitForResponse(Client client, String commandName, Arguments arguments) throws TException {
    // TODO This should likely be changed to run of a AtomicBoolean used for
    // the status of commands.
    String executionId = null;
    while (true) {
      try {
        if (executionId == null) {
          return client.execute(commandName, arguments);
        } else {
          return client.reconnect(executionId);
        }
      } catch (BlurException e) {
        throw e;
      } catch (TimeoutException e) {
        executionId = e.getExecutionId();
        LOG.info("Execution fetch timed out, reconnecting using [{0}].", executionId);
      } catch (TException e) {
        throw e;
      }
    }
  }

  private Set<Shard> getShardsOnServer(Server server, Set<String> tables, Map<String, Set<Shard>> shards)
      throws IOException {
    Set<Shard> serverLayout = _layoutFactory.getServerLayout(server);
    Set<Shard> result = new HashSet<Shard>();
    for (Shard shard : serverLayout) {
      if (isValid(shard, tables, shards)) {
        result.add(shard);
      }
    }
    return result;
  }

  private boolean isValid(Shard shard, Set<String> tables, Map<String, Set<Shard>> shards) {
    String table = shard.getTable();
    if (!tables.contains(table)) {
      return false;
    }
    Set<Shard> shardSet = shards.get(table);
    if (shardSet.isEmpty()) {
      return true;
    } else {
      return shardSet.contains(shard);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Map<Server, Future<T>> readServersAsync(final Args args,
      Class<? extends IndexReadCombiningCommand<?, T>> clazz) throws IOException {
    final String commandName = _manager.getCommandName((Class<? extends Command>) clazz);
    Command command = _manager.getCommandObject(commandName);
    Map<Server, Future<T>> futureMap = new HashMap<Server, Future<T>>();
    Set<String> tables = _manager.getTables(command, args);
    Map<String, Set<Shard>> shards = _manager.getShards(command, args, tables);
    Map<Server, Client> clientMap = getClientMap(command, args, tables, shards);
    for (Entry<Server, Client> e : clientMap.entrySet()) {
      Server server = e.getKey();
      final Client client = e.getValue();
      Future<T> future = _manager.submitToExecutorService(new Callable<T>() {
        @Override
        public T call() throws Exception {
          Arguments arguments = CommandUtil.toArguments(args);
          Response response = waitForResponse(client, commandName, arguments);
          ValueObject valueObject = response.getValue();
          return (T) CommandUtil.toObject(valueObject);
        }
      });
      futureMap.put(server, future);
    }
    return futureMap;
  }

  private <K, T> Map<K, T> processFutures(Class<?> clazz, Map<K, Future<T>> futures, Map<K, T> result)
      throws IOException {
    Throwable firstError = null;
    for (Entry<K, Future<T>> e : futures.entrySet()) {
      K key = e.getKey();
      Future<T> future = e.getValue();
      T value;
      try {
        value = future.get();
        result.put(key, value);
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (firstError == null) {
          firstError = cause;
        }
        LOG.error("Unknown call while executing command [{0}] on server or shard [{1}]", clazz, key);
      }
    }
    if (firstError != null) {
      throw new IOException(firstError);
    }
    return result;
  }

  @Override
  public BlurConfiguration getBlurConfiguration(String table) throws IOException {
    return _tableContextFactory.getTableContext(table).getBlurConfiguration();
  }

  @Override
  public Configuration getConfiguration(String table) throws IOException {
    return _tableContextFactory.getTableContext(table).getConfiguration();
  }

  @Override
  public <T> T readIndex(Args args, Class<? extends IndexReadCommand<T>> clazz) throws IOException {
    Future<T> future = readIndexAsync(args, clazz);
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public <T> Future<T> readIndexAsync(Args args, Class<? extends IndexReadCommand<T>> clazz) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }
}
