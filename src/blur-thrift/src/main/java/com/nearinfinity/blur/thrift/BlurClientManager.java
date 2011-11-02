/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.Proxy.Type;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class BlurClientManager {

  private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
  private static final int MAX_RETIRES = 2;
  private static final long BACK_OFF_TIME = TimeUnit.MILLISECONDS.toMillis(250);

  private static Map<Connection, BlockingQueue<Client>> clientPool = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  
  @SuppressWarnings("unchecked")
  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command) throws Exception {
    int retries = 0;
    while (true) {
      Blur.Client client = getClient(connection);
      if (client == null) {
        throw new BlurException("Host [" + connection + "] can not be contacted.", null);
      }
      try {
        return command.call((CLIENT) client);
      } catch (Exception e) {
        trashClient(connection, client);
        client = null;
        if (retries >= MAX_RETIRES) {
          LOG.error("No more retries [{0}] out of [{1}]", retries, MAX_RETIRES);
          throw e;
        }
        retries++;
        LOG.error("Retrying call [{0}] retry [{1}] out of [{2}] message [{3}]", command, retries, MAX_RETIRES, e.getMessage());
        Thread.sleep(BACK_OFF_TIME);
      } finally {
        if (client != null) {
          returnClient(connection, client);
        }
      }
    }
  }
  
  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command) throws Exception {
    return execute(new Connection(connectionStr),command);
  }

  private static void returnClient(Connection connection, Client client) throws InterruptedException {
    clientPool.get(connection).put(client);
  }

  private static void trashClient(Connection connection, Client client) {
    client.getInputProtocol().getTransport().close();
    client.getOutputProtocol().getTransport().close();
  }

  private static Client getClient(Connection connection) throws InterruptedException, TTransportException, IOException {
    BlockingQueue<Client> blockingQueue;
    synchronized (clientPool) {
      blockingQueue = clientPool.get(connection);
      if (blockingQueue == null) {
        blockingQueue = new LinkedBlockingQueue<Client>();
        clientPool.put(connection, blockingQueue);
      }
    }
    if (blockingQueue.isEmpty()) {
      return newClient(connection);
    }
    return blockingQueue.take();
  }

  private static Client newClient(Connection connection) throws TTransportException, IOException {
    String host = connection.getHost();
    int port = connection.getPort();
    TTransport trans;
    if (connection.isProxy()) {
      Proxy proxy = new Proxy(Type.SOCKS, new InetSocketAddress(connection.getProxyHost(),connection.getProxyPort()));
      Socket socket = new Socket(proxy);
      socket.connect(new InetSocketAddress(host, port));
      trans = new TSocket(socket);
    } else {
      trans = new TSocket(host, port);
      try {
        trans.open();
      } catch (Exception e) {
        LOG.error("Error trying to open connection to [{0}]", connection);
        return null;
      }
    }
    
    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
    Client client = new Client(proto);
    return client;
  }

}
