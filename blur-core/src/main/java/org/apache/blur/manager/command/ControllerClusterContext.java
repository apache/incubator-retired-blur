package org.apache.blur.manager.command;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.blur.server.TableContext;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class ControllerClusterContext extends ClusterContext implements Closeable {

  private final Args _args;
  private final TableContext _tableContext;

  public ControllerClusterContext(TableContext tableContext, Args args) {
    _tableContext = tableContext;
    _args = args;
  }

  @Override
  public Args getArgs() {
    return _args;
  }

  @Override
  public TableContext getTableContext() {
    return _tableContext;
  }

  @Override
  public <T> Map<Shard, T> readIndexes(Args args, Class<? extends IndexReadCommand<T>> clazz) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public <T> Map<Server, T> readServers(Args args, Class<? extends IndexReadCombiningCommand<?, T>> clazz) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public <T> T writeIndex(Args args, Class<? extends IndexWriteCommand<T>> clazz) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void close() throws IOException {
    throw new RuntimeException("Not Implemented");
  }

}
