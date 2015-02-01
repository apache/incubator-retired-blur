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

import org.apache.blur.command.commandtype.ClusterExecuteCommand;
import org.apache.blur.command.commandtype.ClusterExecuteServerReadCommand;
import org.apache.blur.command.commandtype.ClusterServerReadCommand;
import org.apache.blur.command.commandtype.ClusterIndexReadCommand;
import org.apache.blur.command.commandtype.ServerReadCommand;
import org.apache.blur.command.commandtype.IndexReadCommand;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.BlurClient.BlurClientInvocationHandler;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.ClientPool;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.TimeoutException;
import org.apache.blur.trace.Tracer;

public class CommandRunner {
  public static Connection[] getConnection(Iface client) {
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

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command) throws IOException, BlurException, TException {
    Iface client = BlurClient.getClient();
    return run(command, getConnection(client));
  }

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, String connectionStr) throws IOException,
      BlurException, TException {
    Iface client = BlurClient.getClient(connectionStr);
    return run(command, getConnection(client));
  }

  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Blur.Iface client) throws IOException,
      BlurException, TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Shard, T> run(IndexReadCommand<T> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (Map<Shard, T>) runInternal((Command<?>) command, connection);
  }

  public static <T> Map<Server, T> run(ServerReadCommand<?, T> command) throws IOException, BlurException, TException {
    Iface client = BlurClient.getClient();
    return run(command, getConnection(client));
  }

  public static <T> Map<Server, T> run(ServerReadCommand<?, T> command, String connectionStr) throws IOException,
      BlurException, TException {
    Iface client = BlurClient.getClient(connectionStr);
    return run(command, getConnection(client));
  }

  public static <T> Map<Server, T> run(ServerReadCommand<?, T> command, Blur.Iface client) throws IOException,
      BlurException, TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<Server, T> run(ServerReadCommand<?, T> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (Map<Server, T>) runInternal((Command<?>) command, connection);
  }

  public static <T1, T2> T2 run(ClusterIndexReadCommand<T1, T2> command) throws IOException, BlurException, TException {
    return run(command, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2> T2 run(ClusterIndexReadCommand<T1, T2> command, String connectioStr) throws IOException,
      BlurException, TException {
    return (T2) runInternal((Command<?>) command, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T1, T2> T2 run(ClusterIndexReadCommand<T1, T2> command, Blur.Iface client) throws IOException,
      BlurException, TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T1, T2> T2 run(ClusterIndexReadCommand<T1, T2> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (T2) runInternal((Command<?>) command, connection);
  }

  public static <T> T run(ClusterServerReadCommand<T> command) throws IOException, BlurException, TException {
    return run(command, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterServerReadCommand<T> command, String connectioStr) throws IOException, BlurException,
      TException {
    return (T) runInternal((Command<?>) command, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterServerReadCommand<T> command, Blur.Iface client) throws IOException, BlurException,
      TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterServerReadCommand<T> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (T) runInternal((Command<?>) command, connection);
  }

  public static <T> T run(ClusterExecuteCommand<T> command) throws IOException, BlurException, TException {
    return run(command, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteCommand<T> command, String connectioStr) throws IOException, BlurException,
      TException {
    return (T) runInternal((Command<?>) command, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterExecuteCommand<T> command, Blur.Iface client) throws IOException, BlurException,
      TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteCommand<T> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (T) runInternal((Command<?>) command, connection);
  }

  public static <T> T run(ClusterExecuteServerReadCommand<T> command) throws IOException, BlurException, TException {
    return run(command, getConnection(BlurClient.getClient()));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteServerReadCommand<T> command, String connectioStr) throws IOException,
      BlurException, TException {
    return (T) runInternal((Command<?>) command, getConnection(BlurClient.getClient(connectioStr)));
  }

  public static <T> T run(ClusterExecuteServerReadCommand<T> command, Blur.Iface client) throws IOException,
      BlurException, TException {
    return run(command, getConnection(client));
  }

  @SuppressWarnings("unchecked")
  public static <T> T run(ClusterExecuteServerReadCommand<T> command, Connection... connection) throws IOException,
      BlurException, TException {
    return (T) runInternal((Command<?>) command, connection);
  }

  public static Object runInternal(Command<?> command, Connection... connectionsArray) throws TTransportException,
      IOException, BlurException, TimeoutException, TException {
    BlurObjectSerDe serde = new BlurObjectSerDe();
    Arguments arguments = CommandUtil.toArguments(command, serde);
    Object thriftObject = runInternalReturnThriftObject(command.getName(), arguments, connectionsArray);
    return serde.fromSupportedThriftObject(thriftObject);
  }

  public static Object runInternalReturnThriftObject(String name, Arguments arguments, Connection... connectionsArray)
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
        Long executionId = null;
        Response response;
        INNER: while (true) {
          Tracer tracer = BlurClientManager.setupClientPreCall(client);
          try {
            if (executionId == null) {
              response = client.execute(name, arguments);
            } else {
              response = client.reconnect(executionId);
            }
            break INNER;
          } catch (TimeoutException te) {
            executionId = te.getInstanceExecutionId();
          } finally {
            if (tracer != null) {
              tracer.done();
            }
          }
        }
        return CommandUtil.fromThriftResponseToObject(response);
      } finally {
        clientPool.returnClient(connection, client);
      }
    }
    throw new BlurException("All connections bad. [" + connections + "]", null, ErrorType.UNKNOWN);
  }
}
