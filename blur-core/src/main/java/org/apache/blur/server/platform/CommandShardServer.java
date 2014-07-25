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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.AdHocByteCodeCommandRequest;
import org.apache.blur.thrift.generated.AdHocByteCodeCommandResponse;
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
    return execute(command, getBlurIndexes(tables, tablesToExecute));
  }

  private <T1, T2> T2 execute(Command<T1, T2> command, Map<String, Map<String, BlurIndex>> indexes) throws IOException,
      CommandException {
    List<Future<T1>> futures = new ArrayList<Future<T1>>();
    for (Entry<String, Map<String, BlurIndex>> tableEntry : indexes.entrySet()) {
      Map<String, BlurIndex> shards = tableEntry.getValue();
      for (Entry<String, BlurIndex> shardEntry : shards.entrySet()) {
        final BlurIndex blurIndex = shardEntry.getValue();
        Callable<T1> callable = command.createCallable(blurIndex);
        futures.add(_executorService.submit(callable));
      }
    }

    CommandException commandException = new CommandException();
    boolean error = false;
    List<T1> results = new ArrayList<T1>();
    for (Future<T1> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        commandException.addSuppressed(e);
        error = true;
      } catch (ExecutionException e) {
        commandException.addSuppressed(e.getCause());
        error = true;
      }
    }
    if (error) {
      throw commandException;
    }
    return command.mergeIntermediate(results);

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
    Object fieldValue = request.getFieldValue();
    BlurCommandResponse blurCommandResponse = new BlurCommandResponse();
    if (fieldValue instanceof AdHocByteCodeCommandRequest) {
      AdHocByteCodeCommandRequest commandRequest = request.getAdHocByteCodeCommandRequest();
      AdHocByteCodeCommandResponse response = execute(tables, commandRequest);
      blurCommandResponse.setAdHocByteCodeCommandResponse(response);
    } else {
      throw new BException("Not implemented.");
    }
    return blurCommandResponse;
  }

  public AdHocByteCodeCommandResponse execute(Set<String> tables, AdHocByteCodeCommandRequest commandRequest)
      throws BlurException, IOException, CommandException {
    // @TODO handle libraries
    Set<String> tablesToInvoke = commandRequest.getTablesToInvoke();
    Map<String, ByteBuffer> classData = commandRequest.getClassData();
    ClassLoader classLoader = CommandUtils.getClassLoader(classData);
    Object[] args = CommandUtils.getArgs(classLoader, commandRequest.getArguments());
    Command<?, ?> command = CommandUtils.toObjectViaSerialization(classLoader, commandRequest.getInstanceData());
    Object object = execute(tables, command, tablesToInvoke, args);
    Value value = CommandUtils.toValue(object);

    AdHocByteCodeCommandResponse adHocByteCodeCommandResponse = new AdHocByteCodeCommandResponse();
    adHocByteCodeCommandResponse.setResult(value);
    return adHocByteCodeCommandResponse;
  }

  @Override
  public void close() throws IOException {
    _executorService.shutdownNow();
  }

}
