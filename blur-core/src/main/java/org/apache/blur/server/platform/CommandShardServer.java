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
package org.apache.blur.server.platform;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandRequest;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandResponse;
import org.apache.blur.thrift.generated.BlurCommandRequest;
import org.apache.blur.thrift.generated.BlurCommandResponse;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;

public class CommandShardServer implements Closeable {

  private final IndexServer _indexServer;
  private final ExecutorService _executorService;

  public CommandShardServer(IndexServer indexServer, ExecutorService executorService) {
    _indexServer = indexServer;
    _executorService = executorService;
  }

  public CommandShardServer(IndexServer indexServer) {
    this(indexServer, Executors.newCachedThreadPool());
  }

  public <T1, T2> T2 execute(Set<String> tables, Command<T1, T2> command, Set<String> tablesToExecute, Object... args)
      throws CommandException, IOException {
    command.setArgs(args);
    command.setExecutorService(_executorService);
    return command.process(getBlurIndexes(tables, tablesToExecute));
  }

  private Map<String, Map<String, BlurIndex>> getBlurIndexes(Set<String> tables, Set<String> tablesToExecute)
      throws IOException {
    Map<String, Map<String, BlurIndex>> indexes = new HashMap<String, Map<String, BlurIndex>>();
    for (String table : tables) {
      Map<String, BlurIndex> map = _indexServer.getIndexes(table);
      indexes.put(table, map);
    }
    return indexes;
  }

  public BlurCommandResponse execute(Set<String> tables, BlurCommandRequest request) throws BlurException, IOException,
      CommandException {
    // @TODO deal with different command types.
    Set<String> tablesToInvoke = new HashSet<String>();
    tablesToInvoke.add("test");
    Object fieldValue = request.getFieldValue();
    BlurCommandResponse blurCommandResponse = new BlurCommandResponse();
    if (fieldValue instanceof AdhocByteCodeCommandRequest) {
      AdhocByteCodeCommandRequest commandRequest = request.getAdhocByteCodeCommandRequest();
      AdhocByteCodeCommandResponse response = execute(tables, commandRequest, tablesToInvoke);
      blurCommandResponse.setAdhocByteCodeCommandResponse(response);
    } else {
      throw new BException("Not implemented.");
    }
    return blurCommandResponse;
  }

  public AdhocByteCodeCommandResponse execute(Set<String> tables, AdhocByteCodeCommandRequest commandRequest,
      Set<String> tablesToInvoke) throws BlurException, IOException, CommandException {
    // @TODO handle libraries

    Map<String, ByteBuffer> classData = commandRequest.getClassData();
    ClassLoader classLoader = CommandUtils.getClassLoader(classData);
    Object[] args = CommandUtils.getArgs(classLoader, commandRequest.getArguments());
    Command<?, ?> command = CommandUtils.toObjectViaSerialization(classLoader, commandRequest.getInstanceData());
    Object object = execute(tables, command, tablesToInvoke, args);
    Value value = CommandUtils.toValue(object);

    AdhocByteCodeCommandResponse adhocByteCodeCommandResponse = new AdhocByteCodeCommandResponse();
    adhocByteCodeCommandResponse.setResult(value);
    return adhocByteCodeCommandResponse;
  }

  @Override
  public void close() throws IOException {
    _executorService.shutdownNow();
  }

}
