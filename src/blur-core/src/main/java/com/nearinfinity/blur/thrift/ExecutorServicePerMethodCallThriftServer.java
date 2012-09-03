package com.nearinfinity.blur.thrift;

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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorServicePerMethodCallThriftServer extends THsHaServer {

  private static final Object[] ARGS = new Object[] {};
  private static final Logger LOGGER = LoggerFactory.getLogger(THsHaServer.class.getName());
  private Method method;

  public static class Args extends THsHaServer.Args {
    private int workerThreads = 5;
    private int stopTimeoutVal = 60;
    private TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
    private ExecutorService executorService = null;
    private Map<String, ExecutorService> methodCallsToExecutorService;

    public Map<String, ExecutorService> getMethodCallsToExecutorService() {
      return methodCallsToExecutorService;
    }

    public void setMethodCallsToExecutorService(Map<String, ExecutorService> methodCallsToExecutorService) {
      this.methodCallsToExecutorService = methodCallsToExecutorService;
    }

    public Args(TNonblockingServerTransport transport) {
      super(transport);
    }

    public Args workerThreads(int i) {
      workerThreads = i;
      return this;
    }

    public int getWorkerThreads() {
      return workerThreads;
    }

    public int getStopTimeoutVal() {
      return stopTimeoutVal;
    }

    public Args stopTimeoutVal(int stopTimeoutVal) {
      this.stopTimeoutVal = stopTimeoutVal;
      return this;
    }

    public TimeUnit getStopTimeoutUnit() {
      return stopTimeoutUnit;
    }

    public Args stopTimeoutUnit(TimeUnit stopTimeoutUnit) {
      this.stopTimeoutUnit = stopTimeoutUnit;
      return this;
    }

    public ExecutorService getExecutorService() {
      return executorService;
    }

    public Args executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }
  }

  private ExecutorService invokerStandard;
  private Map<String, ExecutorService> methodCallsToExecutorService;

  public ExecutorServicePerMethodCallThriftServer(Args args) {
    super(args);
    try {
      method = FrameBuffer.class.getDeclaredMethod("getInputTransport", new Class[] {});
      method.setAccessible(true);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    invokerStandard = args.executorService == null ? createInvokerPool(args) : args.executorService;
    methodCallsToExecutorService = args.methodCallsToExecutorService == null ? new HashMap<String, ExecutorService>() : args.methodCallsToExecutorService;
  }

  @Override
  protected boolean requestInvoke(FrameBuffer frameBuffer) {
    try {
      String name;
      try {
        name = readMethodCall(frameBuffer);
      } catch (TException e) {
        LOGGER.error("Unexpected exception while invoking!", e);
        return false;
      }
      Runnable invocation = getRunnable(frameBuffer);
      ExecutorService executorService = methodCallsToExecutorService.get(name);
      if (executorService == null) {
        invokerStandard.execute(invocation);
      } else {
        executorService.execute(invocation);
      }
      return true;
    } catch (RejectedExecutionException rx) {
      LOGGER.warn("ExecutorService rejected execution!", rx);
      return false;
    }
  }

  private String readMethodCall(FrameBuffer frameBuffer) throws TException {
    TTransport transport = getInputTransport(frameBuffer);
    TProtocol inProt = inputProtocolFactory_.getProtocol(transport);
    try {
      TMessage tMessage = inProt.readMessageBegin();
      return tMessage.name;
    } finally {
      transport.close();
    }

  }

  private TTransport getInputTransport(FrameBuffer frameBuffer) {
    try {
      return (TTransport) method.invoke(frameBuffer, ARGS);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
