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
import java.net.Proxy.Type;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;

public class BlurClientManager {

  private static final Object NULL = new Object();
  
  private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
  private static final int MAX_RETRIES = 5;
  private static final long BACK_OFF_TIME = TimeUnit.MILLISECONDS.toMillis(250);
  private static final long MAX_BACK_OFF_TIME = TimeUnit.SECONDS.toMillis(10);
  private static final long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);
  
  private static Map<Connection, BlockingQueue<Client>> clientPool = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  private static Thread daemon;
  private static AtomicBoolean running = new AtomicBoolean(true);
  private static Map<Connection,Object> badConnections = new ConcurrentHashMap<Connection, Object>();
  
  static {
    startDaemon();
  }
  
  private static void startDaemon() {
    daemon = new Thread(new Runnable() {
      private Set<Connection> good = new HashSet<Connection>();
      @Override
      public void run() {
        while (running.get()) {
          good.clear();
          Set<Connection> badConns = badConnections.keySet();
          for (Connection connection : badConns) {
            if (isConnectionGood(connection)) {
              good.add(connection);
            }
          }
          for (Connection connection : good) {
            badConnections.remove(connection);
          }
          try {
            Thread.sleep(ONE_SECOND);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    daemon.setDaemon(true);
    daemon.setName("Blur-Client-Manager-Connection-Checker");
    daemon.start();
  }
  
  protected static boolean isConnectionGood(Connection connection) {
    try {
      returnClient(connection, getClient(connection));
      return true;
    } catch (TTransportException e) {
      LOG.debug("Connection [{0}] is still bad.", connection);
    } catch (IOException e) {
      LOG.debug("Connection [{0}] is still bad.", connection);
    }
    return false;
  }

  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command) throws BlurException, TException, IOException {
    return execute(connection, command, MAX_RETRIES, BACK_OFF_TIME, MAX_BACK_OFF_TIME);
  }
  
  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command, int maxRetries, long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    return execute(Arrays.asList(connection), command, maxRetries, backOffTime, maxBackOffTime);
  }
  
  public static <CLIENT, T> T execute(List<Connection> connections, AbstractCommand<CLIENT, T> command) throws BlurException, TException, IOException {
    return execute(connections, command, MAX_RETRIES, BACK_OFF_TIME, MAX_BACK_OFF_TIME);
  }
  
  private static class LocalResources {
    AtomicInteger retries = new AtomicInteger();
    AtomicReference<Blur.Client> client = new AtomicReference<Client>();
    List<Connection> shuffledConnections = new ArrayList<Connection>();
    Random random = new Random();
  }
  
//  private static ThreadLocal<LocalResources> resources = new ThreadLocal<BlurClientManager.LocalResources>() {
//    @Override
//    protected LocalResources initialValue() {
//      return new LocalResources();
//    }
//  };
  
  @SuppressWarnings("unchecked")
  public static <CLIENT, T> T execute(List<Connection> connections, AbstractCommand<CLIENT, T> command, int maxRetries, long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
//    LocalResources localResources = resources.get();
    LocalResources localResources = null;
    try {
      if (localResources == null) {
        localResources = new LocalResources();
      }
//      resources.set(null);
      AtomicReference<Client> client = localResources.client;
      Random random = localResources.random;
      AtomicInteger retries = localResources.retries;
      List<Connection> shuffledConnections = localResources.shuffledConnections;
      
      retries.set(0);
      shuffledConnections.addAll(connections);
      
      Collections.shuffle(shuffledConnections, random);
      while (true) {
        for (Connection connection : shuffledConnections) {
          if (isBadConnection(connection)) {
            continue;
          }
          client.set(null);
          try {
            client.set(getClient(connection));
          } catch (IOException e) {
            if (handleError(connection,client,retries,command,e,maxRetries,backOffTime,maxBackOffTime)) {
              throw e;
            } else {
              markBadConnection(connection);
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
    } finally {
//      resources.set(localResources);
    }
  }

  private static void markBadConnection(Connection connection) {
    LOG.info("Marking bad connection [{0}]",connection);    
    badConnections.put(connection, NULL);
  }

  private static boolean isBadConnection(Connection connection) {
    return badConnections.containsKey(connection);
  }

  private static <CLIENT,T> boolean handleError(Connection connection, AtomicReference<Blur.Client> client, AtomicInteger retries, AbstractCommand<CLIENT, T> command, Exception e, int maxRetries, long backOffTime, long maxBackOffTime) {
    if (client.get() != null) {
      trashConnections(connection,client);
      markBadConnection(connection);
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

  public static void sleep(long backOffTime, long maxBackOffTime, int retry, int maxRetries) {
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
    return execute(getCommands(connectionStr),command,maxRetries,backOffTime,maxBackOffTime);
  }
  
  private static List<Connection> getCommands(String connectionStr) {
    int start = 0;
    int index = connectionStr.indexOf(',');
    if (index >= 0) {
      List<Connection> connections = new ArrayList<Connection>();
      while (index >= 0) {
        connections.add(new Connection(connectionStr.substring(start, index)));
        start = index + 1;
        index = connectionStr.indexOf(',', start);
      }
      connections.add(new Connection(connectionStr.substring(start)));
      return connections;
    }
    return Arrays.asList(new Connection(connectionStr));
  }

  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command) throws BlurException, TException, IOException {
    return execute(getCommands(connectionStr),command);
  }

  private static void returnClient(Connection connection, AtomicReference<Blur.Client> client) {
    returnClient(connection, client.get());
  }
  
  private static void returnClient(Connection connection, Blur.Client client) {
    try {
      clientPool.get(connection).put(client);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void trashConnections(Connection connection, AtomicReference<Client> c) {
    BlockingQueue<Client> blockingQueue;
    synchronized (clientPool) {
      blockingQueue = clientPool.put(connection,new LinkedBlockingQueue<Client>());
      try {
        blockingQueue.put(c.get());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    
    LOG.info("Trashing client for connections [{0}]",connection);
    for (Client client : blockingQueue) {
      client.getInputProtocol().getTransport().close();
      client.getOutputProtocol().getTransport().close();
    }
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
