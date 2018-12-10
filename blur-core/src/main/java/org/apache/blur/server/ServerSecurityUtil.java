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
package org.apache.blur.server;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;

public class ServerSecurityUtil {

  private static final Log LOG = LogFactory.getLog(ServerSecurityUtil.class);

  public static Iface applySecurity(final Iface iface, final List<ServerSecurityFilter> serverSecurityList,
      final boolean shardServer) {
    if (serverSecurityList == null || serverSecurityList.isEmpty()) {
      LOG.info("No server security configured.");
      return iface;
    }
    for (ServerSecurityFilter serverSecurity : serverSecurityList) {
      LOG.info("Server security configured with [{0}] class [{1}].", serverSecurity, serverSecurity.getClass());
    }
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        BlurServerContext blurServerContext;
        if (shardServer) {
          blurServerContext = ShardServerContext.getShardServerContext();
        } else {
          blurServerContext = ControllerServerContext.getControllerServerContext();
        }
        InetSocketAddress remoteSocketAddress = (InetSocketAddress) blurServerContext.getRemoteSocketAddress();
        InetAddress address = remoteSocketAddress.getAddress();
        int port = remoteSocketAddress.getPort();
        User user = UserContext.getUser();
        for (ServerSecurityFilter serverSecurity : serverSecurityList) {
          if (!serverSecurity.canAccess(method, args, user, address, port)) {
            throw new BException("ACCESS DENIED for User [{0}] method [{1}].", user, method.getName());
          }
        }
        try {
          return method.invoke(iface, args);
        } catch (InvocationTargetException e) {
          throw e.getTargetException();
        }
      }
    };
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class }, handler);
  }

}
