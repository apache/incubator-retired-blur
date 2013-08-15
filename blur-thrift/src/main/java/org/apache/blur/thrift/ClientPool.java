package org.apache.blur.thrift;

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur.Client;

public class ClientPool {

  private static final Log LOG = LogFactory.getLog(ClientPool.class);
  private final Map<Connection, BlockingQueue<Client>> _clientPool = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  private int _maxConnectionsPerHost = Integer.MAX_VALUE;

  // private long _idleTimeBeforeClosingClient = Long.MAX_VALUE;

  public void returnClient(Connection connection, Client client) {
    try {
      getQueue(connection).put(client);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private BlockingQueue<Client> getQueue(Connection connection) {
    BlockingQueue<Client> blockingQueue = _clientPool.get(connection);
    synchronized (_clientPool) {
      blockingQueue = _clientPool.get(connection);
      if (blockingQueue == null) {
        blockingQueue = getNewQueue();
        _clientPool.put(connection, blockingQueue);
      }
    }
    return _clientPool.get(connection);
  }

  public void trashConnections(Connection connection, Client client) {
    BlockingQueue<Client> blockingQueue;
    synchronized (_clientPool) {
      blockingQueue = _clientPool.put(connection, getNewQueue());
      try {
        blockingQueue.put(client);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    LOG.info("Trashing client for connections [{0}]", connection);
    for (Client c : blockingQueue) {
      close(c);
    }
  }

  private BlockingQueue<Client> getNewQueue() {
    return new LinkedBlockingQueue<Client>(_maxConnectionsPerHost);
  }

  public Client getClient(Connection connection) throws TTransportException, IOException {
    BlockingQueue<Client> blockingQueue = getQueue(connection);
    if (blockingQueue.isEmpty()) {
      return newClient(connection);
    }
    try {
      return blockingQueue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Client newClient(Connection connection) throws TTransportException, IOException {
    String host = connection.getHost();
    int port = connection.getPort();
    TSocket trans;
    Socket socket;
    if (connection.isProxy()) {
      Proxy proxy = new Proxy(Type.SOCKS, new InetSocketAddress(connection.getProxyHost(), connection.getProxyPort()));
      socket = new Socket(proxy);
    } else {
      socket = new Socket();
    }
    int timeout = connection.getTimeout();
    socket.setTcpNoDelay(true);
    socket.setSoTimeout(timeout);
    socket.connect(new InetSocketAddress(host, port), timeout);
    trans = new TSocket(socket);

    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
    Client client = new Client(proto);
    return client;
  }

  public void close(Client client) {
    client.getInputProtocol().getTransport().close();
    client.getOutputProtocol().getTransport().close();
  }

}
