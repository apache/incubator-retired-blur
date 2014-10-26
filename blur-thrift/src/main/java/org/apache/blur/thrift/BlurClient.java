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
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT_DEFAULT;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.zookeeper.WatchChildren;
import org.apache.blur.zookeeper.WatchChildren.OnChange;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.zookeeper.ZooKeeper;

public class BlurClient {

  private static final Log LOG = LogFactory.getLog(BlurClient.class);

  public static class BlurClientInvocationHandler implements InvocationHandler {

    private final List<Connection> _connections;
    private final int _maxRetries;
    private final long _backOffTime;
    private final long _maxBackOffTime;

    public BlurClientInvocationHandler(List<Connection> connections, int maxRetries, long backOffTime,
        long maxBackOffTime) {
      _connections = connections;
      _maxRetries = maxRetries;
      _backOffTime = backOffTime;
      _maxBackOffTime = maxBackOffTime;
    }

    public BlurClientInvocationHandler(List<Connection> connections) {
      this(connections, BlurClientManager.MAX_RETRIES, BlurClientManager.BACK_OFF_TIME,
          BlurClientManager.MAX_BACK_OFF_TIME);
    }

    public List<Connection> getConnections() {
      return _connections;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
      return BlurClientManager.execute(_connections, new BlurCommand<Object>() {
        @Override
        public Object call(Client client) throws BlurException, TException {
          try {
            return method.invoke(client, args);
          } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException instanceof BlurException) {
              throw (BlurException) targetException;
            }
            if (targetException instanceof TException) {
              throw (TException) targetException;
            }
            throw new RuntimeException(targetException);
          }
        }
      }, _maxRetries, _backOffTime, _maxBackOffTime);
    }
  }

  private static volatile BlurConfiguration _blurConfiguration;
  private static final List<Connection> _connections = new CopyOnWriteArrayList<Connection>();
  private static volatile ZooKeeper _zooKeeper;
  private static WatchChildren _watchConntrollers;

  public static Iface getClient() {
    try {
      return getClient(getBlurConfiguration());
    } catch (IOException e) {
      throw new RuntimeException("Unable to load configurations.", e);
    }
  }

  private static synchronized BlurConfiguration getBlurConfiguration() throws IOException {
    if (_blurConfiguration == null) {
      _blurConfiguration = new BlurConfiguration();
    }
    return _blurConfiguration;
  }

  public static Iface getClient(BlurConfiguration conf) {
    return getClient(getOnlineControllers(conf));
  }

  /**
   * Returns a client interface to Blur based on the connectionStr.
   * 
   * <pre>
   * Blur.Iface client = Blur.getClient(&quot;controller1:40010,controller2:40010&quot;);
   * </pre>
   * 
   * The connectionStr also supports passing a proxy host/port (e.g. a SOCKS
   * proxy configuration):
   * 
   * <pre>
   * Blur.Iface client = Blur.getClient(&quot;host1:port/proxyhost1:proxyport&quot;);
   * </pre>
   * 
   * @param connectionStr
   *          - a comma-delimited list of host:port of Shard Controllers.
   * @return
   */
  public static Iface getClient(String connectionStr) {
    List<Connection> connections = BlurClientManager.getConnections(connectionStr);
    return getClient(connections);
  }

  public static Iface getClient(String connectionStr, int maxRetries, long backOffTime, long maxBackOffTime) {
    List<Connection> connections = BlurClientManager.getConnections(connectionStr);
    return getClient(connections, maxRetries, backOffTime, maxBackOffTime);
  }

  public static Iface getClient(Connection connection) {
    return getClient(Arrays.asList(connection));
  }

  public static Iface getClient(List<Connection> connections) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections));
  }

  public static Iface getClient(Connection connection, int maxRetries, long backOffTime, long maxBackOffTime) {
    return getClient(Arrays.asList(connection), maxRetries, backOffTime, maxBackOffTime);
  }

  public static Iface getClient(List<Connection> connections, int maxRetries, long backOffTime, long maxBackOffTime) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections, maxRetries, backOffTime, maxBackOffTime));
  }

  private static List<Connection> getOnlineControllers(BlurConfiguration conf) {
    setupZooKeeper(conf);
    return _connections;
  }

  private static void setupZooKeeper(BlurConfiguration conf) {
    if (_zooKeeper == null) {
      String zkConn = conf.getExpected(BLUR_ZOOKEEPER_CONNECTION);
      int zkSessionTimeout = conf.getInt(BLUR_ZOOKEEPER_TIMEOUT, BLUR_ZOOKEEPER_TIMEOUT_DEFAULT);
      try {
        _zooKeeper = ZkUtils.newZooKeeper(zkConn, zkSessionTimeout);
        setConnections(_zooKeeper.getChildren(ZookeeperPathConstants.getOnlineControllersPath(), false));
        _watchConntrollers = new WatchChildren(_zooKeeper, ZookeeperPathConstants.getOnlineControllersPath());
        _watchConntrollers.watch(new OnChange() {
          @Override
          public void action(List<String> children) {
            setConnections(children);
          }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
          @Override
          public void run() {
            closeZooKeeper();
          }
        }));
      } catch (Exception e) {
        throw new RuntimeException("Unknown error while trying to connect to ZooKeeper and fetch controllers.", e);
      }
    }
  }

  public static void closeZooKeeper() {
    if (_watchConntrollers != null) {
      try {
        _watchConntrollers.close();
      } catch (Exception e) {
        LOG.error("Unknown error while closing Controller Watcher.", e);
      }
    }
    if (_zooKeeper != null) {
      try {
        _zooKeeper.close();
      } catch (Exception e) {
        LOG.error("Unknown error while closing ZooKeeper client.", e);
      }
    }
  }

  private static void setConnections(List<String> children) {
    Set<Connection> goodConnections = new HashSet<Connection>();
    for (String s : children) {
      Connection connection = new Connection(s);
      goodConnections.add(connection);
      if (!_connections.contains(connection)) {
        _connections.add(connection);
      }
    }
    Set<Connection> badConnections = new HashSet<Connection>();
    for (Connection c : _connections) {
      if (!goodConnections.contains(c)) {
        badConnections.add(c);
      }
    }
    _connections.removeAll(badConnections);
  }

}
