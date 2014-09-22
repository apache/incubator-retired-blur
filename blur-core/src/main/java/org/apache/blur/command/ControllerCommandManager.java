package org.apache.blur.command;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.blur.server.LayoutFactory;
import org.apache.blur.server.TableContextFactory;
import org.apache.hadoop.conf.Configuration;

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
@SuppressWarnings("unchecked")
public class ControllerCommandManager extends BaseCommandManager {

  public ControllerCommandManager(String tmpPath, String commandPath, int workerThreadCount, int driverThreadCount,
      long connectionTimeout, Configuration configuration) throws IOException {
    super(tmpPath, commandPath, workerThreadCount, driverThreadCount, connectionTimeout, configuration);
  }

  public Response execute(TableContextFactory tableContextFactory, LayoutFactory layoutFactory, String commandName,
      final Args args) throws IOException, TimeoutException {
    final ClusterContext context = createCommandContext(tableContextFactory, layoutFactory, args);
    final Command command = getCommandObject(commandName);
    if (command == null) {
      throw new IOException("Command with name [" + commandName + "] not found.");
    }
    return submitDriverCallable(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        // For those commands that do not implement cluster command, run them in
        // a
        // base impl.
        if (command instanceof ClusterCommand) {
          return executeClusterCommand(context, command);
        } else if (command instanceof IndexReadCombiningCommand) {
          return executeIndexReadCombiningCommand(args, context, command);
        } else if (command instanceof IndexReadCommand) {
          return executeIndexReadCommand(args, context, command);
        } else {
          throw new IOException("Command type of [" + command.getClass() + "] not supported.");
        }
      }
    });
  }

  private Response executeClusterCommand(ClusterContext context, Command command) throws IOException {
    ClusterCommand<Object> clusterCommand = (ClusterCommand<Object>) command;
    Object object = clusterCommand.clusterExecute(context);
    return Response.createNewAggregateResponse(object);
  }

  private Response executeIndexReadCommand(Args args, ClusterContext context, Command command) throws IOException {
    Class<? extends IndexReadCommand<Object>> clazz = (Class<? extends IndexReadCommand<Object>>) command.getClass();
    Map<Shard, Object> result = context.readIndexes(args, clazz);
    return Response.createNewShardResponse(result);
  }

  private Response executeIndexReadCombiningCommand(Args args, ClusterContext context, Command command)
      throws IOException {
    Class<? extends IndexReadCombiningCommand<Object, Object>> clazz = (Class<? extends IndexReadCombiningCommand<Object, Object>>) command
        .getClass();
    Map<Server, Object> result = context.readServers(args, clazz);
    return Response.createNewServerResponse(result);
  }

  private ClusterContext createCommandContext(TableContextFactory tableContextFactory, LayoutFactory layoutFactory,
      Args args) throws IOException {
    return new ControllerClusterContext(tableContextFactory, layoutFactory, args, this);
  }

}
