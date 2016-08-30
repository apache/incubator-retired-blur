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
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TFramedTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.User;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Trace.TraceId;
import org.apache.blur.trace.Tracer;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.ThreadValue;

public class BlurClientManager {

  private static final Object NULL = new Object();

  private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
  public static final int MAX_RETRIES = 5;
  public static final long BACK_OFF_TIME = TimeUnit.MILLISECONDS.toMillis(250);
  public static final long MAX_BACK_OFF_TIME = TimeUnit.SECONDS.toMillis(10);
  private static final long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);

  private static ClientPool _clientPool = new ClientPool();
  private static Thread _daemon;
  private static AtomicBoolean _running = new AtomicBoolean(true);
  private static Map<Connection, Object> _badConnections = new ConcurrentHashMap<Connection, Object>();

  static {
    startDaemon();
  }

  public static void setClientPool(ClientPool clientPool) {
    _clientPool = clientPool;
  }

  public static ClientPool getClientPool() {
    return _clientPool;
  }

  private static void startDaemon() {
    _daemon = new Thread(new Runnable() {
      private Set<Connection> good = new HashSet<Connection>();

      @Override
      public void run() {
        while (_running.get()) {
          good.clear();
          Set<Connection> badConns = _badConnections.keySet();
          for (Connection connection : badConns) {
            if (isConnectionGood(connection)) {
              good.add(connection);
            }
          }
          for (Connection connection : good) {
            _badConnections.remove(connection);
          }
          try {
            Thread.sleep(ONE_SECOND);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _daemon.setDaemon(true);
    _daemon.setName("Blur-Client-Manager-Connection-Checker");
    _daemon.start();
  }

  protected static boolean isConnectionGood(Connection connection) {
    try {
      returnClient(connection, _clientPool.getClient(connection));
      return true;
    } catch (TTransportException e) {
      LOG.debug("Connection [{0}] is still bad.", connection);
    } catch (IOException e) {
      LOG.debug("Connection [{0}] is still bad.", connection);
    }
    return false;
  }

  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command) throws BlurException,
      TException, IOException {
    return execute(connection, command, MAX_RETRIES, BACK_OFF_TIME, MAX_BACK_OFF_TIME);
  }

  public static <CLIENT, T> T execute(Connection connection, AbstractCommand<CLIENT, T> command, int maxRetries,
      long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    return execute(Arrays.asList(connection), command, maxRetries, backOffTime, maxBackOffTime);
  }

  public static <CLIENT, T> T execute(List<Connection> connections, AbstractCommand<CLIENT, T> command)
      throws BlurException, TException, IOException {
    return execute(connections, command, MAX_RETRIES, BACK_OFF_TIME, MAX_BACK_OFF_TIME);
  }

  private static class LocalResources {
    AtomicInteger retries = new AtomicInteger();
    AtomicReference<Blur.Client> client = new AtomicReference<Client>();
    List<Connection> shuffledConnections = new ArrayList<Connection>();
  }

  private static ThreadValue<Random> _random = new ThreadValue<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  @SuppressWarnings("unchecked")
  public static <CLIENT, T> T execute(List<Connection> connections, AbstractCommand<CLIENT, T> command, int maxRetries,
      long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    Tracer traceSetup = Trace.trace("execute - setup");
    LocalResources localResources = new LocalResources();
    AtomicReference<Client> client = localResources.client;
    AtomicInteger retries = localResources.retries;
    List<Connection> shuffledConnections = localResources.shuffledConnections;

    retries.set(0);
    shuffledConnections.addAll(connections);

    Collections.shuffle(shuffledConnections, _random.get());
    boolean allBad = true;
    int connectionErrorCount = 0;
    traceSetup.done();
    while (true) {
      for (Connection connection : shuffledConnections) {
        Tracer traceConnectionSetup = Trace.trace("execute - connection setup");
        try {
          if (isBadConnection(connection)) {
            continue;
          }
          client.set(null);
          try {
            client.set(_clientPool.getClient(connection));
          } catch (IOException e) {
            if (handleError(connection, client, retries, command, e, maxRetries, backOffTime, maxBackOffTime)) {
              throw e;
            } else {
              markBadConnection(connection);
              continue;
            }
          }
        } finally {
          traceConnectionSetup.done();
        }
        Tracer trace = null;
        try {
          trace = setupClientPreCall(client.get());
          T result = command.call((CLIENT) client.get(), connection);
          allBad = false;
          if (command.isDetachClient()) {
            // if the is detach client is set then the command will return the
            // client to the pool.
            client.set(null);
          }
          return result;
        } catch (RuntimeException e) {
          Throwable cause = e.getCause();
          if (cause instanceof TTransportException) {
            TTransportException t = (TTransportException) cause;
            if (handleError(connection, client, retries, command, t, maxRetries, backOffTime, maxBackOffTime)) {
              Throwable c = t.getCause();
              if (cause instanceof SocketTimeoutException) {
                throw new BlurException(c.getMessage(), BException.toString(c), ErrorType.REQUEST_TIMEOUT);
              }
              throw t;
            }
          } else {
            throw e;
          }
        } catch (TTransportException e) {
          if (handleError(connection, client, retries, command, e, maxRetries, backOffTime, maxBackOffTime)) {
            Throwable c = e.getCause();
            if (c instanceof SocketTimeoutException) {
              throw new BlurException(c.getMessage(), BException.toString(c), ErrorType.REQUEST_TIMEOUT);
            }
            throw e;
          }
        } finally {
          if (trace != null) {
            trace.done();
          }
          if (client.get() != null) {
            returnClient(connection, client);
          }
        }
      }
      if (allBad) {
        connectionErrorCount++;
        LOG.error("All connections are bad [{0}] for [{1}].", connectionErrorCount, connections);
        if (connectionErrorCount >= maxRetries) {
          throw new BadConnectionException("Could not connect to controller/shard server [" + connections
              + "]. All connections are bad.");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new BException("Unknown error.", e);
        }
      }
    }
  }

  public static Tracer setupClientPreCall(Client client) throws TException {
    User user = UserConverter.toThriftUser(UserContext.getUser());
    client.setUser(user);
    TraceId traceId = Trace.getTraceId();
    if (traceId != null) {
      client.startTrace(traceId.getRootId(), traceId.getRequestId());
      return Trace.trace("thrift client", Trace.param("connection", getConnectionStr(client)));
    }
    return null;
  }

  private static String getConnectionStr(Client client) {
    TTransport transport = client.getInputProtocol().getTransport();
    if (transport instanceof TFramedTransport) {
      TFramedTransport framedTransport = (TFramedTransport) transport;
      transport = framedTransport.getTransport();
    }
    if (transport instanceof TSocket) {
      TSocket tsocket = (TSocket) transport;
      Socket socket = tsocket.getSocket();
      SocketAddress localSocketAddress = socket.getLocalSocketAddress();
      SocketAddress remoteSocketAddress = socket.getRemoteSocketAddress();
      return localSocketAddress.toString() + ":" + remoteSocketAddress.toString();
    }
    return "unknown";
  }

  private static void markBadConnection(Connection connection) {
    LOG.info("Marking bad connection [{0}]", connection);
    _badConnections.put(connection, NULL);
  }

  public static boolean isBadConnection(Connection connection) {
    return _badConnections.containsKey(connection);
  }

  private static <CLIENT, T> boolean handleError(Connection connection, AtomicReference<Blur.Client> client,
      AtomicInteger retries, AbstractCommand<CLIENT, T> command, Exception e, int maxRetries, long backOffTime,
      long maxBackOffTime) {
    if (client.get() != null) {
      if (e != null) {
        LOG.debug("Error [{0}]", e, e.getMessage());
      }
      trashConnections(connection, client);
      markBadConnection(connection);
      client.set(null);
    }
    if (retries.get() > maxRetries) {
      LOG.error("No more retries [{0}] out of [{1}]", retries, maxRetries);
      return true;
    }
    LOG.error("Retrying call [{0}] retry [{1}] out of [{2}] message [{3}]", command, retries.get(), maxRetries,
        e.getMessage());
    sleep(backOffTime, maxBackOffTime, retries.get(), maxRetries);
    retries.incrementAndGet();
    return false;
  }

  public static void sleep(long backOffTime, long maxBackOffTime, int retry, int maxRetries) {
    if (maxRetries == 0) {
      return;
    }
    long extra = (maxBackOffTime - backOffTime) / maxRetries;
    long sleep = backOffTime + (extra * retry);
    LOG.info("Backing off call for [{0} ms]", sleep);
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command, int maxRetries,
      long backOffTime, long maxBackOffTime) throws BlurException, TException, IOException {
    return execute(getConnections(connectionStr), command, maxRetries, backOffTime, maxBackOffTime);
  }

  public static List<Connection> getConnections(String connectionStr) {
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

  public static <CLIENT, T> T execute(String connectionStr, AbstractCommand<CLIENT, T> command) throws BlurException,
      TException, IOException {
    return execute(getConnections(connectionStr), command);
  }

  public static void returnClient(Connection connection, AtomicReference<Blur.Client> client) {
    returnClient(connection, client.get());
  }

  private static void trashConnections(Connection connection, AtomicReference<Client> c) {
    _clientPool.trashConnections(connection, c.get());
  }

  public static void returnClient(Connection connection, Blur.Client client) {
    _clientPool.returnClient(connection, client);
  }

  public static void close(Client client) {
    ClientPool.close(client);
  }

  public static Client newClient(Connection connection) throws TTransportException, IOException {
    return _clientPool.newClient(connection);
  }

}
