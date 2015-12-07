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
import static org.apache.blur.utils.BlurConstants.BLUR_CLIENTPOOL_CLIENT_MAX_CONNECTIONS_PER_HOST;
import static org.apache.blur.utils.BlurConstants.BLUR_CLIENTPOOL_CLIENT_STALE_THRESHOLD;
import static org.apache.blur.utils.BlurConstants.BLUR_THRIFT_MAX_FRAME_SIZE;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.SafeClientGen;
import org.apache.blur.thrift.sasl.SaslHelper;

public class ClientPool {

  private static final Log LOG = LogFactory.getLog(ClientPool.class);
  private static final Map<Connection, BlockingQueue<Client>> _connMap = new ConcurrentHashMap<Connection, BlockingQueue<Client>>();
  private static final int _maxFrameSize;
  private static final int _maxConnectionsPerHost;

  private static final long _idleTimeBeforeClosingClient;
  private static final long _clientPoolCleanFrequency;
  private static final Timer _master;
  private static final BlurConfiguration _configurationFromClassPath;
  private BlurConfiguration _configuration = _configurationFromClassPath;

  static {
    try {
      _configurationFromClassPath = new BlurConfiguration();
      int maxConnectionsPerHost = _configurationFromClassPath.getInt(BLUR_CLIENTPOOL_CLIENT_MAX_CONNECTIONS_PER_HOST,
          64);
      if (maxConnectionsPerHost < 1) {
        LOG.fatal("Max connections per host cannot be less than 1 current value [{0}] using 1.", maxConnectionsPerHost);
        maxConnectionsPerHost = 1;
      }
      _maxConnectionsPerHost = maxConnectionsPerHost;
      _idleTimeBeforeClosingClient = TimeUnit.SECONDS.toNanos(_configurationFromClassPath.getLong(
          BLUR_CLIENTPOOL_CLIENT_STALE_THRESHOLD, 30));
      _clientPoolCleanFrequency = TimeUnit.SECONDS.toMillis(_configurationFromClassPath.getLong(
          BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY, 10));
      _maxFrameSize = _configurationFromClassPath.getInt(BLUR_THRIFT_MAX_FRAME_SIZE, 16384000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    _master = checkAndRemoveStaleClients();
  }

  public void setBlurConfiguration(BlurConfiguration configuration) {
    _configuration = configuration;
  }

  public static void close() {
    _master.cancel();
    _master.purge();
  }

  private static Timer checkAndRemoveStaleClients() {
    Timer master = new Timer("Blur-Client-Connection-Cleaner", true);
    master.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          for (Entry<Connection, BlockingQueue<Client>> e : _connMap.entrySet()) {
            testConnections(e.getKey(), e.getValue());
          }
        } catch (Throwable t) {
          LOG.error("Unknown error while trying to clean up connections.", t);
        }
      }
    }, getClientPoolCleanFrequency(), getClientPoolCleanFrequency());
    return master;
  }

  private static void testConnections(Connection connection, BlockingQueue<Client> clients) {
    int size = clients.size();
    LOG.debug("Testing clients for connection [{0}] has size of [{1}]", connection, size);
    for (int i = 0; i < size; i++) {
      WeightedClient weightedClient = (WeightedClient) clients.poll();
      if (weightedClient == null) {
        return;
      }
      if (weightedClient.isStale()) {
        if (testClient(connection, weightedClient)) {
          tryToReturnToQueue(clients, weightedClient);
        } else {
          LOG.error("Closing potentially bad client [{0}]", weightedClient);
          close(weightedClient);
        }
      } else {
        tryToReturnToQueue(clients, weightedClient);
      }
    }
  }

  private static void tryToReturnToQueue(BlockingQueue<Client> clients, WeightedClient weightedClient) {
    LOG.debug("Offering client [{0}] to queue.", weightedClient);
    if (!clients.offer(weightedClient)) {
      // Close client
      LOG.info("Too many clients in pool, closing client [{0}]", weightedClient);
      close(weightedClient);
    }
  }

  private class WeightedClient extends SafeClientGen {
    private long _lastUse = System.nanoTime();
    private final String _id;

    public WeightedClient(TProtocol prot, String id) {
      super(prot);
      _id = id;
    }

    public void touch() {
      _lastUse = System.nanoTime();
    }

    public boolean isStale() {
      long diff = System.nanoTime() - _lastUse;
      return diff >= getClientIdleTimeThreshold();
    }

    @Override
    public String toString() {
      return _id;
    }

  }

  private static long getClientIdleTimeThreshold() {
    return _idleTimeBeforeClosingClient;
  }

  private static long getClientPoolCleanFrequency() {
    return _clientPoolCleanFrequency;
  }

  public void returnClient(Connection connection, Client client) {
    BlockingQueue<Client> queue = getQueue(connection);
    WeightedClient weightedClient = (WeightedClient) client;
    weightedClient.touch();
    tryToReturnToQueue(queue, weightedClient);
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
    LOG.debug("Trashing client for connections [{0}]", connection);
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
      LOG.debug("New client for connection [{0}]", connection);
      return newClient(connection);
    }
    while (true) {
      WeightedClient client = (WeightedClient) blockingQueue.poll();
      if (client == null) {
        LOG.debug("New client for connection [{0}]", connection);
        return newClient(connection);
      }
      if (client.isStale()) {
        // Test client
        if (testClient(connection, client)) {
          return refresh(client);
        }
      } else {
        return refresh(client);
      }
    }
  }

  private Client refresh(WeightedClient client) throws IOException {
    try {
      client.refresh();
    } catch (TException e) {
      throw new IOException(e);
    }
    return client;
  }

  public Client newClient(Connection connection) throws TTransportException, IOException {
    String host = connection.getHost();
    int port = connection.getPort();

    TProtocol proto;
    Socket socket;
    int timeout = connection.getTimeout();
    if (SaslHelper.isSaslEnabled(_configuration)) {
      if (connection.isProxy()) {
        throw new IOException("Proxy connections are not allowed when SASL is enabled.");
      }
      TSocket transport = new TSocket(host, port, timeout);
      TTransport tSaslClientTransport = SaslHelper.getTSaslClientTransport(_configuration, transport);
      tSaslClientTransport.open();
      socket = transport.getSocket();
      proto = new TCompactProtocol(tSaslClientTransport);
    } else {
      if (connection.isProxy()) {
        Proxy proxy = new Proxy(Type.SOCKS, new InetSocketAddress(connection.getProxyHost(), connection.getProxyPort()));
        socket = new Socket(proxy);
      } else {
        socket = new Socket();
      }
      socket.setTcpNoDelay(true);
      socket.setSoTimeout(timeout);
      socket.connect(new InetSocketAddress(host, port), timeout);
      TSocket trans = new TSocket(socket);
      proto = new TBinaryProtocol(new TFramedTransport(trans, _maxFrameSize));
    }
    return new WeightedClient(proto, getIdentifer(socket));
  }

  private String getIdentifer(Socket socket) {
    SocketAddress localSocketAddress = socket.getLocalSocketAddress();
    SocketAddress remoteSocketAddress = socket.getRemoteSocketAddress();
    return localSocketAddress.toString() + " -> " + remoteSocketAddress.toString();
  }

  private static boolean testClient(Connection connection, WeightedClient weightedClient) {
    LOG.debug("Testing client, could be stale. Client [{0}] for connection [{1}]", weightedClient, connection);
    try {
      weightedClient.refresh();
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
