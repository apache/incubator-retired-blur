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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.BlurClient.BlurClientInvocationHandler;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.ClientPool;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.TimeoutException;

public class CommandRunner {
  private static Connection[] getConnection(Iface client) {
    if (client instanceof Proxy) {
      InvocationHandler invocationHandler = Proxy.getInvocationHandler(client);
      if (invocationHandler instanceof BlurClientInvocationHandler) {
        BlurClientInvocationHandler handler = (BlurClientInvocationHandler) invocationHandler;
        return handler.getConnections().toArray(new Connection[] {});
      }
    }
    if (client == null) {
      throw new RuntimeException("Client cannot be null.");
    }
    throw new RuntimeException("Unknown client class [" + client.getClass() + "]");
  }

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Args arguments) throws IOException, BlurException,
      TException {
    Iface client = BlurClient.getClient();
    return run(command, arguments, getConnection(client));
  }

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Args arguments, String connectionStr)
      throws IOException, BlurException, TException {
    Iface client = BlurClient.getClient(connectionStr);
    return run(command, arguments, getConnection(client));
  }

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Args arguments, Blur.Iface client)
      throws IOException, BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Args arguments, Connection... connection)
      throws IOException, BlurException, TException {
    return (Map<Shard, T>) runInternal((Command<?>) command, arguments, connection);
  }

  public static <T> Map<Server, T> run(IndexReadCombiningCommand<?, T> command, Args arguments) throws IOException,
      BlurException, TException {
    Iface client = BlurClient.getClient();
    return run(command, arguments, getConnection(client));
  }

  public static <T> Map<Server, T> run(IndexReadCombiningCommand<?, T> command, Args arguments, String connectionStr)
      throws IOException, BlurException, TException {
    Iface client = BlurClient.getClient(connectionStr);
    return run(command, arguments, getConnection(client));
  }

  public static <T> Map<Server, T> run(IndexReadCombiningCommand<?, T> command, Args arguments, Blur.Iface client)
      throws IOException, BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Server, T> run(IndexReadCombiningCommand<?, T> command, Args arguments,
      Connection... connection) throws IOException, BlurException, TException {
    return (Map<Server, T>) runInternal((Command<?>) command, arguments, connection);
  }

  public static <T1, T2> T2 run(ClusterReadCommand<T1, T2> command, Args arguments) throws IOException, BlurException,
      TException {
    return run(command, arguments, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2> T2 run(ClusterReadCommand<T1, T2> command, Args arguments, String connectioStr)
      throws IOException, BlurException, TException {
    return (T2) runInternal((Command<?>) command, arguments, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T1, T2> T2 run(ClusterReadCommand<T1, T2> command, Args arguments, Blur.Iface client)
      throws IOException, BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2> T2 run(ClusterReadCommand<T1, T2> command, Args arguments, Connection... connection)
      throws IOException, BlurException, TException {
    return (T2) runInternal((Command<?>) command, arguments, connection);
  }

  public static <T> T run(ClusterReadCombiningCommand<T> command, Args arguments) throws IOException, BlurException,
      TException {
    return run(command, arguments, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterReadCombiningCommand<T> command, Args arguments, String connectioStr)
      throws IOException, BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterReadCombiningCommand<T> command, Args arguments, Blur.Iface client)
      throws IOException, BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterReadCombiningCommand<T> command, Args arguments, Connection... connection)
      throws IOException, BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, connection);
  }

  public static <T> T run(ClusterExecuteCommand<T> command, Args arguments) throws IOException, BlurException,
      TException {
    return run(command, arguments, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteCommand<T> command, Args arguments, String connectioStr) throws IOException,
      BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterExecuteCommand<T> command, Args arguments, Blur.Iface client) throws IOException,
      BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteCommand<T> command, Args arguments, Connection... connection)
      throws IOException, BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, connection);
  }

  public static <T> T run(ClusterExecuteReadCombiningCommand<T> command, Args arguments) throws IOException,
      BlurException, TException {
    return run(command, arguments, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteReadCombiningCommand<T> command, Args arguments, String connectioStr)
      throws IOException, BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterExecuteReadCombiningCommand<T> command, Args arguments, Blur.Iface client)
      throws IOException, BlurException, TException {
    return run(command, arguments, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteReadCombiningCommand<T> command, Args arguments, Connection... connection)
      throws IOException, BlurException, TException {
    return (T) runInternal((Command<?>) command, arguments, connection);
  }

  private static Object runInternal(Command<?> command, Args arguments, Connection... connectionsArray)
      throws TTransportException, IOException, BlurException, TimeoutException, TException {
    List<Connection> connections = new ArrayList<Connection>(Arrays.asList(connectionsArray));
    Collections.shuffle(connections);
    for (Connection connection : connections) {
      if (BlurClientManager.isBadConnection(connection)) {
        continue;
      }
      ClientPool clientPool = BlurClientManager.getClientPool();
      Client client = clientPool.getClient(connection);
      try {
        Response response = client.execute(command.getName(), CommandUtil.toArguments(arguments));
        return CommandUtil.fromThriftResponseToObject(response);
      } finally {
        clientPool.returnClient(connection, client);
      }
    }
    throw new BlurException("All connections bad. [" + connections + "]", null, ErrorType.UNKNOWN);
  }
}