package org.apache.blur.server;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class ShardServerEventHandler implements TServerEventHandler {
  
  private static final Log LOG = LogFactory.getLog(ShardServerEventHandler.class);

  @Override
  public void preServe() {
LOG.info("setup");
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    LOG.info("Client connected");
    return new ShardServerContext();
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    LOG.info("Client disconnected");
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    LOG.info("Method called");
  }

}
