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

import static org.apache.blur.utils.BlurConstants.BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY;
import static org.apache.blur.utils.BlurConstants.BLUR_CLIENTPOOL_CLIENT_CLOSE_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_MAX_FRAME_SIZE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.Socket;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.SafeClientGen;

public class ClientPool {

  private static final Log LOG = LogFactory.getLog(ClientPool.class);
  private static final Map<Connection, BlockingQueue<Client>> _connMap = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  private int _maxConnectionsPerHost = Integer.MAX_VALUE;
  private static final int _maxFrameSize;
  private static AtomicBoolean _running = new AtomicBoolean(true);
  private static long _idleTimeBeforeClosingClient;
  private static long _clientPoolCleanFrequency;
  private static Thread _master;

  static {
    try {
      BlurConfiguration config = new BlurConfiguration();
      _idleTimeBeforeClosingClient = config.getLong(BLUR_CLIENTPOOL_CLIENT_CLOSE_THRESHOLD,
          TimeUnit.SECONDS.toNanos(30));
      _clientPoolCleanFrequency = config.getLong(BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY, TimeUnit.SECONDS.toMillis(3));
      _maxFrameSize = config.getInt(BLUR_THRIFT_MAX_FRAME_SIZE, 16384000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    checkAndRemoveStaleClients();
  }

  private static void checkAndRemoveStaleClients() {
    _master = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          for (Entry<Connection, BlockingQueue<Client>> e : _connMap.entrySet()) {
            testConnections(e.getKey(), e.getValue());
          }
          try {
            Thread.sleep(getClientPoolCleanFrequency());
          } catch (InterruptedException e) {
            return;
          }
        }
      }

      private void testConnections(Connection connection, BlockingQueue<Client> clients) {
        LOG.debug("Testing clients for connection [{0}]", connection);
        int size = clients.size();
        for (int i = 0; i < size; i++) {
          WeightedClient weightedClient = (WeightedClient) clients.poll();
          if (weightedClient == null) {
            return;
          }
          if (weightedClient.isStale()) {
            if (testClient(connection, weightedClient)) {
              tryToReturnToQueue(clients, weightedClient);
            } else {
              close(weightedClient);
            }
          } else {
            tryToReturnToQueue(clients, weightedClient);
          }
        }
      }

      private void tryToReturnToQueue(BlockingQueue<Client> clients, WeightedClient weightedClient) {
        if (!clients.offer(weightedClient)) {
          // Close client
          close(weightedClient);
        }
      }
    });
    _master.setDaemon(true);
    _master.setName("Blur-Client-Connection-Cleaner");
    _master.start();
  }

  private class WeightedClient extends SafeClientGen {
    private long _lastUse = System.nanoTime();

    public WeightedClient(TProtocol prot) {
      super(prot);
    }

    public void touch() {
      _lastUse = System.nanoTime();
    }

    public boolean isStale() {
      long diff = System.nanoTime() - _lastUse;
      return diff >= getClientIdleTimeThreshold();
    }
  }

  private static long getClientIdleTimeThreshold() {
    return _idleTimeBeforeClosingClient;
  }

  private static long getClientPoolCleanFrequency() {
    return _clientPoolCleanFrequency;
  }

  public void returnClient(Connection connection, Client client) {
    ((WeightedClient) client).touch();
    if (!getQueue(connection).offer(client)) {
      // Close client
      close(client);
    }
  }

  private BlockingQueue<Client> getQueue(Connection connection) {
    BlockingQueue<Client> blockingQueue = _connMap.get(connection);
    if (blockingQueue != null) {
      return blockingQueue;
    }
    synchronized (_connMap) {
      blockingQueue = _connMap.get(connection);
      if (blockingQueue == null) {
        blockingQueue = getNewQueue();
        _connMap.put(connection, blockingQueue);
      }
    }
    return _connMap.get(connection);
  }

  public void trashConnections(Connection connection, Client client) {
    BlockingQueue<Client> blockingQueue;
    synchronized (_connMap) {
      blockingQueue = _connMap.put(connection, getNewQueue());
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

  /**
   * Get a client from the pool or creates a new one if the pool is empty. Also
   * the clients are tested before being returned.
   * 
   * @param connection
   * @return
   * @throws TTransportException
   * @throws IOException
   */
  public Client getClient(Connection connection) throws TTransportException, IOException {
    BlockingQueue<Client> blockingQueue = getQueue(connection);
    if (blockingQueue.isEmpty()) {
      return newClient(connection);
    }
    while (true) {
      WeightedClient client = (WeightedClient) blockingQueue.poll();
      if (client == null) {
        return newClient(connection);
      }
      if (client.isStale()) {
        // Test client
        if (testClient(connection, client)) {
          return client;
        }
      } else {
        return client;
      }
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

    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans, _maxFrameSize));
    return new WeightedClient(proto);
  }

  private static boolean testClient(Connection connection, WeightedClient weightedClient) {
    LOG.debug("Testing client, could be stale. Client [{0}] for connection [{1}]", weightedClient, connection);
    try {
      weightedClient.ping();
      weightedClient.touch();
      return true;
    } catch (TException e) {
      LOG.error("Client test failed. Destroying client. Client [{0}] for connection [{1}]", weightedClient, connection);
      return false;
    }
  }

  public static void close(Client client) {
    try {
      client.getInputProtocol().getTransport().close();
      client.getOutputProtocol().getTransport().close();
    } catch (Exception e) {
      LOG.error("Error during closing of client [{0}].", client);
    }
  }
}
