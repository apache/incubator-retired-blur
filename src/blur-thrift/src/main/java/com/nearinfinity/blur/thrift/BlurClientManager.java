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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class BlurClientManager {

  private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
  private static final int MAX_RETRIES = 5;
  private static final long BACK_OFF_TIME = TimeUnit.MILLISECONDS.toMillis(250);
  private static final long MAX_BACK_OFF_TIME = TimeUnit.SECONDS.toMillis(10);

  private static Map<Connection, BlockingQueue<Client>> clientPool = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  
  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command) throws BlurException, TException, IOException {
    return execute(connection, command, MAX_RETRIES, BACK_OFF_TIME, MAX_BACK_OFF_TIME);
  }
  
  @SuppressWarnings("unchecked")
  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command, int maxRetries, long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    AtomicInteger retries = new AtomicInteger(0);
    AtomicReference<Blur.Client> client = new AtomicReference<Client>();
    while (true) {
      client.set(null);
      try {
        client.set(getClient(connection));
      } catch (IOException e) {
        if (handleError(connection,client,retries,command,e,maxRetries,backOffTime,maxBackOffTime)) {
          throw e;
        } else {
          continue;
        }
      }
      try {
        return command.call((CLIENT) client.get());
      } catch (TTransportException e) {
        if (handleError(connection,client,retries,command,e,maxRetries,backOffTime,maxBackOffTime)) {
          throw e;
        }
      } finally {
        if (client.get() != null) {
          returnClient(connection, client);
        }
      }
    }
  }
  
  private static <CLIENT,T> boolean handleError(Connection connection, AtomicReference<Blur.Client> client, AtomicInteger retries, AbstractCommand<CLIENT, T> command, Exception e, int maxRetries, long backOffTime, long maxBackOffTime) {
    if (client.get() != null) {
      trashClient(connection, client);
      client.set(null);
    }
    if (retries.get() > maxRetries) {
      LOG.error("No more retries [{0}] out of [{1}]", retries, maxRetries);
      return true;
    }
    LOG.error("Retrying call [{0}] retry [{1}] out of [{2}] message [{3}]", command, retries.get(), maxRetries, e.getMessage());
    sleep(backOffTime,maxBackOffTime,retries.get(),maxRetries);
    retries.incrementAndGet();
    return false;
  }

  private static void sleep(long backOffTime, long maxBackOffTime, int retry, int maxRetries) {
    long extra = (maxBackOffTime - backOffTime) / maxRetries;
    long sleep = backOffTime + (extra * retry);
    LOG.info("Backing off call for [{0} ms]",sleep);
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command, int maxRetries, long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    return execute(new Connection(connectionStr),command,maxRetries,backOffTime,maxBackOffTime);
  }
  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command) throws BlurException, TException, IOException {
    return execute(new Connection(connectionStr),command);
  }

  private static void returnClient(Connection connection, AtomicReference<Blur.Client> client) {
    try {
      clientPool.get(connection).put(client.get());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void trashClient(Connection connection, AtomicReference<Blur.Client> client) {
    LOG.info("Trashing client for connection [{0}]",connection);
    Client c = client.get();
    c.getInputProtocol().getTransport().close();
    c.getOutputProtocol().getTransport().close();
  }

  private static Client getClient(Connection connection) throws TTransportException, IOException {
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
    try {
      return blockingQueue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static Client newClient(Connection connection) throws TTransportException, IOException {
    String host = connection.getHost();
    int port = connection.getPort();
    TSocket trans;
    Socket socket;
    if (connection.isProxy()) {
      Proxy proxy = new Proxy(Type.SOCKS, new InetSocketAddress(connection.getProxyHost(),connection.getProxyPort()));
      socket = new Socket(proxy);
    } else {
      socket = new Socket();
    }
    socket.connect(new InetSocketAddress(host, port));
    trans = new TSocket(socket);
    
    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
    Client client = new Client(proto);
    return client;
  }

}
