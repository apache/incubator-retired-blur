package org.apache.blur.command;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.thrift.generated.ValueObject;
import org.apache.blur.trace.Tracer;

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

  private final TableContextFactory _tableContextFactory;
  private final Map<Server, ClientWithConnection> _clientMap;
  private final ControllerCommandManager _manager;
  private final LayoutFactory _layoutFactory;
  private final BlurObjectSerDe _serDe = new BlurObjectSerDe();

  static class ClientWithConnection {
    final Client _client;
    final Connection _connection;

    ClientWithConnection(Client client, Connection connection) {
      _client = client;
      _connection = connection;
    }
  }

  public ControllerClusterContext(TableContextFactory tableContextFactory, LayoutFactory layoutFactory,
      ControllerCommandManager manager) throws IOException {
    _tableContextFactory = tableContextFactory;
    _clientMap = getBlurClientsForCluster(layoutFactory.getServerConnections());
    _manager = manager;
    _layoutFactory = layoutFactory;
  }

  private Map<Server, ClientWithConnection> getBlurClientsForCluster(Set<Connection> serverConnections)
      throws IOException {
    Map<Server, ClientWithConnection> clients = new HashMap<Server, ClientWithConnection>();
    for (Connection serverConnection : serverConnections) {
      try {
        Client client = BlurClientManager.getClientPool().getClient(serverConnection);
        client.refresh();
        ClientWithConnection clientWithConnection = new ClientWithConnection(client, serverConnection);
        clients.put(new Server(serverConnection.getHost() + ":" + serverConnection.getPort()), clientWithConnection);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
    return clients;
  }

  @Override
  public TableContext getTableContext(String table) throws IOException {
    return _tableContextFactory.getTableContext(table);
  }

  @Override
  public <T> Map<Shard, T> readIndexes(IndexRead<T> command) throws IOException {
    Map<Shard, Future<T>> futures = readIndexesAsync(command);
    Map<Shard, T> result = new HashMap<Shard, T>();
    return processFutures((Command<?>) command, futures, result);
  }

  @Override
  public <T> Map<Server, T> readServers(ServerRead<?, T> command) throws IOException {
    Map<Server, Future<T>> futures = readServersAsync(command);
    Map<Server, T> result = new HashMap<Server, T>();
    return processFutures((Command<?>) command, futures, result);
  }

  @Override
  public void close() throws IOException {
    ClientPool clientPool = BlurClientManager.getClientPool();
    Collection<ClientWithConnection> values = _clientMap.values();
    _clientMap.clear();
    for (ClientWithConnection clientWithConnection : values) {
      clientPool.returnClient(clientWithConnection._connection, clientWithConnection._client);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Map<Shard, Future<T>> readIndexesAsync(IndexRead<T> cmd) throws IOException {
    final Command<?> command = (Command<?>) cmd;
    _manager.validate(command);
    Map<Shard, Future<T>> futureMap = new HashMap<Shard, Future<T>>();
    Set<String> tables = command.routeTables(this);
    Set<Shard> shards = command.routeShards(this, tables);
    Map<Server, Client> clientMap = getClientMap(command, tables, shards);

    final Arguments arguments = _manager.toArguments(command);

    CommandStatus originalCommandStatusObject = new CommandStatus(null, command.getName(), arguments, null, null);
    for (Entry<Server, Client> e : clientMap.entrySet()) {
      Server server = e.getKey();
      final Client client = e.getValue();
      Future<Map<Shard, T>> future = _manager.submitToExecutorService(new Callable<Map<Shard, T>>() {
        @Override
        public Map<Shard, T> call() throws Exception {
          Response response = waitForResponse(client, command, arguments);
          Map<Shard, Object> shardToThriftValue = CommandUtil.fromThriftToObjectShard(response.getShardToValue());
          Map<Shard, Object> shardToValue = CommandUtil.fromThriftSupportedObjects(shardToThriftValue, _serDe);
          return (Map<Shard, T>) shardToValue;
        }
      }, command, originalCommandStatusObject, new AtomicBoolean(true));
      for (Shard shard : getShardsOnServer(server, tables, shards)) {
        futureMap.put(shard, new ShardResultFuture<T>(shard, future));
      }
    }
    return futureMap;
  }

  private Map<Server, Client> getClientMap(Command<?> command, Set<String> tables, Set<Shard> shards)
      throws IOException {
    Map<Server, Client> result = new HashMap<Server, Client>();
    for (Entry<Server, ClientWithConnection> e : _clientMap.entrySet()) {
      Server server = e.getKey();
      if (_layoutFactory.isValidServer(server, tables, shards)) {
        result.put(server, e.getValue()._client);
      }
    }
    return result;
  }

  protected static Response waitForResponse(Client client, Command<?> command, Arguments arguments) throws TException {
    // TODO This should likely be changed to run of a AtomicBoolean used for
    // the status of commands.
    Long executionId = null;
    while (true) {
      Tracer tracer = BlurClientManager.setupClientPreCall(client);
      try {
        if (executionId == null) {
          return client.execute(command.getName(), arguments);
        } else {
          return client.reconnect(executionId);
        }
      } catch (BlurException e) {
        throw e;
      } catch (TimeoutException e) {
        executionId = e.getInstanceExecutionId();
        LOG.info("Execution fetch timed out, reconnecting using [{0}].", executionId);
      } catch (TException e) {
        throw e;
      } finally {
        if (tracer != null) {
          tracer.done();
        }
      }
    }
  }

  private Set<Shard> getShardsOnServer(Server server, Set<String> tables, Set<Shard> shards) throws IOException {
    Set<Shard> serverLayout = _layoutFactory.getServerLayout(server);
    Set<Shard> result = new HashSet<Shard>();
    for (Shard shard : serverLayout) {
      if (isValid(shard, tables, shards)) {
        result.add(shard);
      }
    }
    return result;
  }

  private boolean isValid(Shard shard, Set<String> tables, Set<Shard> shards) {
    String table = shard.getTable();
    if (!tables.contains(table)) {
      return false;
    }
    if (shards.isEmpty()) {
      return true;
    } else {
      return shards.contains(shard);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Map<Server, Future<T>> readServersAsync(ServerRead<?, T> cmd) throws IOException {
    final Command<?> command = (Command<?>) cmd;
    _manager.validate(command);
    Map<Server, Future<T>> futureMap = new HashMap<Server, Future<T>>();
    Set<String> tables = command.routeTables(this);
    Set<Shard> shards = command.routeShards(this, tables);
    Map<Server, Client> clientMap = getClientMap(command, tables, shards);
    final Arguments arguments = _manager.toArguments(command);
    CommandStatus originalCommandStatusObject = new CommandStatus(null, command.getName(), arguments, null, null);
    for (Entry<Server, Client> e : clientMap.entrySet()) {
      Server server = e.getKey();
      final Client client = e.getValue();
      Future<T> future = _manager.submitToExecutorService(new Callable<T>() {
        @Override
        public T call() throws Exception {
          Response response = waitForResponse(client, command, arguments);
          ValueObject valueObject = response.getValue();
          Object thriftObject = CommandUtil.toObject(valueObject);
          return (T) _serDe.fromSupportedThriftObject(thriftObject);
        }
      }, command, originalCommandStatusObject, new AtomicBoolean(true));
      futureMap.put(server, future);
    }
    return futureMap;
  }

  private <K, T> Map<K, T> processFutures(Command<?> command, Map<K, Future<T>> futures, Map<K, T> result)
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
        LOG.error("Unknown call while executing command [{0}] on server or shard [{1}]", command, key);
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
  public <T> T readIndex(IndexRead<T> command) throws IOException {
    Future<T> future = readIndexAsync(command);
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public <T> Future<T> readIndexAsync(IndexRead<T> command) throws IOException {
    throw new RuntimeException("Not Implemented.");
  }
}
