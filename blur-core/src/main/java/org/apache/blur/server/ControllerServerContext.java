package org.apache.blur.server;

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
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.thirdparty.thrift_0_9_0.server.ServerContext;

/**
 * The thrift session that holds the connection string of the client.
 */
public class ControllerServerContext implements ServerContext {

  private final static Map<Thread, ControllerServerContext> _threadsToContext = new ConcurrentHashMap<Thread, ControllerServerContext>();
  private final SocketAddress _localSocketAddress;
  private final SocketAddress _remoteSocketAddress;
  private final String _connectionString;

  public ControllerServerContext(SocketAddress localSocketAddress, SocketAddress remoteSocketAddress) {
    _localSocketAddress = localSocketAddress;
    _remoteSocketAddress = remoteSocketAddress;
    _connectionString = _localSocketAddress.toString() + "\t" + _remoteSocketAddress.toString();
  }

  /**
   * Registers the {@link ControllerServerContext} for this thread.
   * 
   * @param context
   *          the {@link ControllerServerContext}.
   */
  public static void registerContextForCall(ControllerServerContext context) {
    _threadsToContext.put(Thread.currentThread(), context);
  }

  /**
   * Gets the {@link ControllerServerContext} for this {@link Thread}.
   * 
   * @return the {@link ControllerServerContext}.
   */
  public static ControllerServerContext getShardServerContext() {
    return _threadsToContext.get(Thread.currentThread());
  }

  public SocketAddress getRocalSocketAddress() {
    return _localSocketAddress;
  }

  public SocketAddress getRemoteSocketAddress() {
    return _remoteSocketAddress;
  }

  public String getConnectionString() {
    return _connectionString;
  }
}
