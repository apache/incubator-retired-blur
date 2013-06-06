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
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.server.ServerContext;
import org.apache.blur.thirdparty.thrift_0_9_0.server.TServerEventHandler;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;

/**
 * {@link ShardServerContext} is the session manager for the shard servers. It
 * allows for reader reuse across method calls.
 */
public class ShardServerEventHandler implements TServerEventHandler {

  private static final Log LOG = LogFactory.getLog(ShardServerEventHandler.class);

  @Override
  public void preServe() {
    LOG.debug("preServe");
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    LOG.debug("Client connected");
    return new ShardServerContext();
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    LOG.debug("Client disconnected");
    ShardServerContext context = (ShardServerContext) serverContext;
    context.close();
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    LOG.debug("Method called");
    ShardServerContext context = (ShardServerContext) serverContext;
    ShardServerContext.registerContextForCall(context);
  }

}
