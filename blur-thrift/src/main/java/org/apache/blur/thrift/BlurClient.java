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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.User;

public class BlurClient {

  private static final Log LOG = LogFactory.getLog(BlurClient.class);
  private static final boolean DEFAULT_SET_USER = false;

  static class BlurClientInvocationHandler implements InvocationHandler {

    private static final String SET_USER = "setUser";
    private final List<Connection> _connections;
    private final int _maxRetries;
    private final long _backOffTime;
    private final long _maxBackOffTime;
    private final boolean _setUser;
    private User user;

    public BlurClientInvocationHandler(List<Connection> connections) {
      this(connections, DEFAULT_SET_USER);
    }

    public BlurClientInvocationHandler(List<Connection> connections, int maxRetries, long backOffTime,
        long maxBackOffTime, boolean setUser) {
      _connections = connections;
      _maxRetries = maxRetries;
      _backOffTime = backOffTime;
      _maxBackOffTime = maxBackOffTime;
      _setUser = setUser;
    }

    public BlurClientInvocationHandler(List<Connection> connections, boolean setUser) {
      this(connections, BlurClientManager.MAX_RETRIES, BlurClientManager.BACK_OFF_TIME,
          BlurClientManager.MAX_BACK_OFF_TIME, setUser);
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
      return BlurClientManager.execute(_connections, new BlurCommand<Object>() {
        @Override
        public Object call(Client client) throws BlurException, TException {
          try {
            if (_setUser && method.getName().equals(SET_USER)) {
              user = (User) args[0];
              LOG.info("Setting the user [{0}] for this client.", user);
              return null;
            } else {
              if (_setUser) {
                client.setUser(user);
              }
              return method.invoke(client, args);
            }
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

  public static Iface getClient(String connectionStr, boolean setUser) {
    List<Connection> connections = BlurClientManager.getConnections(connectionStr);
    return getClient(connections, setUser);
  }

  public static Iface getClient(String connectionStr, int maxRetries, long backOffTime, long maxBackOffTime,
      boolean setUser) {
    List<Connection> connections = BlurClientManager.getConnections(connectionStr);
    return getClient(connections, maxRetries, backOffTime, maxBackOffTime, setUser);
  }

  public static Iface getClient(Connection connection) {
    return getClient(Arrays.asList(connection));
  }

  public static Iface getClient(List<Connection> connections) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections));
  }

  public static Iface getClient(Connection connection, boolean setUser) {
    return getClient(Arrays.asList(connection), setUser);
  }

  public static Iface getClient(List<Connection> connections, boolean setUser) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections, setUser));
  }

  public static Iface getClient(Connection connection, int maxRetries, long backOffTime, long maxBackOffTime) {
    return getClient(Arrays.asList(connection), maxRetries, backOffTime, maxBackOffTime);
  }

  public static Iface getClient(List<Connection> connections, int maxRetries, long backOffTime, long maxBackOffTime) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections, maxRetries, backOffTime, maxBackOffTime, DEFAULT_SET_USER));
  }

  public static Iface getClient(Connection connection, int maxRetries, long backOffTime, long maxBackOffTime,
      boolean setUser) {
    return getClient(Arrays.asList(connection), maxRetries, backOffTime, maxBackOffTime, setUser);
  }

  public static Iface getClient(List<Connection> connections, int maxRetries, long backOffTime, long maxBackOffTime,
      boolean setUser) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
        new BlurClientInvocationHandler(connections, maxRetries, backOffTime, maxBackOffTime, setUser));
  }

}
