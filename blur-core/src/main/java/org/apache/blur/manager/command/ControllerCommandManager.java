package org.apache.blur.manager.command;

import java.io.IOException;

import org.apache.blur.manager.command.primitive.BaseCommand;

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

public class ControllerCommandManager extends BaseCommandManager {

  public ControllerCommandManager(int threadCount) throws IOException {
    super(threadCount);
  }

  public Response execute(String table, String commandName, Args args) {
    ClusterContext context = createCommandContext(table);
    BaseCommand command = getCommand(commandName);
    
    // For those commands that do not implement cluster command, run them in a base impl.
    
    throw new RuntimeException("Not Implemented");
  }

  private BaseCommand getCommand(String commandName) {
    throw new RuntimeException("Not Implemented");
  }

  private ClusterContext createCommandContext(String table) {
    throw new RuntimeException("Not Implemented");
  }

}
