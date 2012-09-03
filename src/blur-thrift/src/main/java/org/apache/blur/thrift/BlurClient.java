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

import org.apache.blur.thrift.commands.BlurCommand;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.thrift.TException;


public class BlurClient {

  static class BlurClientInvocationHandler implements InvocationHandler {

    private List<Connection> connections;

    public BlurClientInvocationHandler(List<Connection> connections) {
      this.connections = connections;
    }

    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args) throws Throwable {
      return BlurClientManager.execute(connections, new BlurCommand<Object>() {
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
      });
    }

  }

  public static Iface getClient(String connectionStr) {
    List<Connection> connections = BlurClientManager.getConnections(connectionStr);
    return getClient(connections);
  }

  public static Iface getClient(Connection connection) {
    return getClient(Arrays.asList(connection));
  }

  public static Iface getClient(List<Connection> connections) {
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class }, new BlurClientInvocationHandler(connections));
  }

}
