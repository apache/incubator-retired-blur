package org.apache.blur.command;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.command.commandtype.ClusterExecuteCommand;
import org.apache.blur.command.commandtype.ClusterIndexReadCommand;
import org.apache.blur.command.commandtype.ClusterServerReadCommand;
import org.apache.blur.command.commandtype.IndexReadCommand;
import org.apache.blur.command.commandtype.ServerReadCommand;
import org.apache.blur.server.LayoutFactory;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.commons.io.IOUtils;
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

  public ControllerCommandManager(File tmpPath, String commandPath, int workerThreadCount, int driverThreadCount,
      long connectionTimeout, Configuration configuration) throws IOException {
    super(tmpPath, commandPath, workerThreadCount, driverThreadCount, connectionTimeout, configuration);
  }

  public Response execute(final TableContextFactory tableContextFactory, LayoutFactory layoutFactory,
      String commandName, ArgumentOverlay argumentOverlay) throws IOException, TimeoutException, ExceptionCollector {
    final ControllerClusterContext context = createCommandContext(tableContextFactory, layoutFactory);
    final Command<?> command = getCommandObject(commandName, argumentOverlay);
    if (command == null) {
      throw new IOException("Command with name [" + commandName + "] not found.");
    }
    return submitDriverCallable(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        // For those commands that do not implement cluster command, run them in
        // a base impl.
        try {

          if (command instanceof IndexReadCommand) {
            return executeIndexReadCommand(context, command);
          }
          if (command instanceof ServerReadCommand) {
            return executeIndexReadCombiningCommand(context, command);
          }
          if (command instanceof ClusterIndexReadCommand) {
            throw new RuntimeException("Not implemented");
          }
          if (command instanceof ClusterServerReadCommand) {
            CombiningContext combiningContext = getCombiningContext(tableContextFactory);
            return executeClusterReadCombiningCommand(context, command, combiningContext);
          }
          if (command instanceof ClusterExecuteCommand) {
            return executeClusterCommand(context, command);
          }

          throw new IOException("Command type of [" + command.getClass() + "] not supported.");
        } finally {
          IOUtils.closeQuietly(context);
        }
      }

    }, command);
  }

  private CombiningContext getCombiningContext(final TableContextFactory tableContextFactory) {
    return new CombiningContext() {

      @Override
      public TableContext getTableContext(String table) throws IOException {
        return tableContextFactory.getTableContext(table);
      }

      @Override
      public BlurConfiguration getBlurConfiguration(String table) throws IOException {
        return getTableContext(table).getBlurConfiguration();
      }
    };
  }

  private Response executeClusterCommand(ClusterContext context, Command<?> command) throws IOException,
      InterruptedException {
    ClusterExecuteCommand<Object> clusterCommand = (ClusterExecuteCommand<Object>) command;
    Object object = clusterCommand.clusterExecute(context);
    return Response.createNewAggregateResponse(object);
  }

  private Response executeIndexReadCommand(ClusterContext context, Command<?> command) throws IOException {
    Map<Shard, Object> result = context.readIndexes((IndexReadCommand<Object>) command);
    return Response.createNewShardResponse(result);
  }

  private Response executeClusterReadCombiningCommand(ClusterContext context, Command<?> command,
      CombiningContext combiningContext) throws IOException, InterruptedException {
    Map<Server, Object> results = context.readServers((ClusterServerReadCommand<Object>) command);
    ClusterServerReadCommand<Object> clusterReadCombiningCommand = (ClusterServerReadCommand<Object>) command;
    Object result = clusterReadCombiningCommand.combine(combiningContext, (Map<? extends Location<?>, Object>) results);
    return Response.createNewAggregateResponse(result);
  }

  private Response executeIndexReadCombiningCommand(ClusterContext context, Command<?> command) throws IOException {
    Map<Server, Object> result = context.readServers((ServerReadCommand<Object, Object>) command);
    return Response.createNewServerResponse(result);
  }

  private ControllerClusterContext createCommandContext(TableContextFactory tableContextFactory,
      LayoutFactory layoutFactory) throws IOException {
    return new ControllerClusterContext(tableContextFactory, layoutFactory, this);
  }

}
