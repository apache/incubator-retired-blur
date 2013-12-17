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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.BlurConfiguration;
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
          TimeUnit.SECONDS.toMillis(30));
      _clientPoolCleanFrequency = config
          .getLong(BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY, TimeUnit.SECONDS.toMillis(300));
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
          try {
            Thread.sleep(getClientPoolCleanFrequency());
            List<Thread> workers = new ArrayList<Thread>();
            int num = 0;
            for (Connection connection : _connMap.keySet()) {
              Thread thread = new PoolWorker(connection);
              thread.setName("client-cleaner_" + ++num);
              thread.start();
              workers.add(thread);
            }
            for (Thread t : workers) {
              t.join();
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    });
    _master.setDaemon(true);
    _master.setName("Blur-Client-Connection-Cleaner");
    _master.start();
  }

  private static class PoolWorker extends Thread {
    private final Connection _connection;

    public PoolWorker(Connection conn) {
      _connection = conn;
    }

    @Override
    public void run() {
      BlockingQueue<Client> bq = _connMap.get(_connection);
      synchronized (_connection) {
        if (!_connMap.get(_connection).isEmpty()) {
          Iterator<Client> it = bq.iterator();
          try {
            while (it.hasNext()) {
              Client client = it.next();
              if (((WeightedClient) client).isStale()) {
                close(client);
                bq.take();
              } else
                break;
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private class WeightedClient extends Client {
    private long _enqueueTime;

    public WeightedClient(TProtocol prot) {
      super(prot);
    }

    public void setEnqueTime(long _currentTime) {
      _enqueueTime = _currentTime;
    }

    public boolean isStale() {
      long diff = System.currentTimeMillis() - _enqueueTime;
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
    try {
      ((WeightedClient) client).setEnqueTime(System.currentTimeMillis());
      getQueue(connection).put(client);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private BlockingQueue<Client> getQueue(Connection connection) {
    BlockingQueue<Client> blockingQueue;
    synchronized (connection) {
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
    synchronized (connection) {
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

    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans, _maxFrameSize));
    return new WeightedClient(proto);
  }

  public static void close(Client client) {
    client.getInputProtocol().getTransport().close();
    client.getOutputProtocol().getTransport().close();
  }
}
