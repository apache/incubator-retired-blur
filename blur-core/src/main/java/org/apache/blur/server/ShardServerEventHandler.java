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
import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.ServerContext;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServerEventHandler;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

/**
 * {@link ShardServerContext} is the session manager for the shard servers. It
 * allows for reader reuse across method calls.
 */
public class ShardServerEventHandler implements TServerEventHandler {

  private static final Log LOG = LogFactory.getLog(ShardServerEventHandler.class);
  private final Meter _connectionMeter;
  private final AtomicLong _connections = new AtomicLong();

  public ShardServerEventHandler() {
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, BLUR, "Connections"), new Gauge<Long>() {
      @Override
      public Long value() {
        return null;
      }
    });
    _connectionMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Connections/s"), "Connections/s",
        TimeUnit.SECONDS);
  }

  @Override
  public void preServe() {
    LOG.debug("preServe");
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output, Object remoteInstance) {
    LOG.debug("Client connected");
    SocketAddress remoteSocketAddress;
    SocketAddress localSocketAddress;
    if (remoteInstance instanceof SelectionKey) {
      SelectionKey selectionKey = (SelectionKey) remoteInstance;
      SocketChannel channel = (SocketChannel) selectionKey.channel();
      Socket socket = channel.socket();
      remoteSocketAddress = socket.getRemoteSocketAddress();
      localSocketAddress = socket.getLocalSocketAddress();
    } else if (remoteInstance instanceof TSocket) {
      TSocket tSocket = (TSocket) remoteInstance;
      Socket socket = tSocket.getSocket();
      remoteSocketAddress = socket.getRemoteSocketAddress();
      localSocketAddress = socket.getLocalSocketAddress();
    } else {
      throw new RuntimeException("Cannot track remote connection off [" + remoteInstance + "]");
    }
    _connectionMeter.mark();
    _connections.incrementAndGet();
    return new ShardServerContext(localSocketAddress, remoteSocketAddress, input, output);
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    LOG.debug("Client disconnected");
    ShardServerContext context = (ShardServerContext) serverContext;
    context.close();
    _connections.decrementAndGet();
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    LOG.debug("Method called");
    ShardServerContext context = (ShardServerContext) serverContext;
    ShardServerContext.registerContextForCall(context);
  }

}
