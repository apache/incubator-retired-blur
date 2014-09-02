package org.apache.blur.manager.command;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.manager.command.cmds.BaseCommand;
import org.apache.blur.manager.command.cmds.DocumentCount;
import org.apache.blur.manager.command.cmds.DocumentCountCombiner;
import org.apache.blur.manager.command.cmds.DocumentCountNoCombine;
import org.apache.blur.manager.command.cmds.TestBlurObjectCommand;

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

public class BaseCommandManager implements Closeable {

  protected final ExecutorService _executorService;
  protected final Map<String, BaseCommand> _command = new ConcurrentHashMap<String, BaseCommand>();
  protected final Map<Class<? extends BaseCommand>, String> _commandNameLookup = new ConcurrentHashMap<Class<? extends BaseCommand>, String>();

  public BaseCommandManager(int threadCount) throws IOException {
    register(DocumentCount.class);
    register(DocumentCountNoCombine.class);
    register(DocumentCountCombiner.class);
    register(TestBlurObjectCommand.class);
    _executorService = Executors.newThreadPool("command-", threadCount);
  }

  @Override
  public void close() throws IOException {
    _executorService.shutdownNow();
  }

  public void register(Class<? extends BaseCommand> commandClass) throws IOException {
    try {
      BaseCommand command = commandClass.newInstance();
      _command.put(command.getName(), command);
      _commandNameLookup.put(commandClass, command.getName());
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  protected BaseCommand getCommandObject(String commandName) {
    return _command.get(commandName);
  }

  protected String getCommandName(Class<? extends BaseCommand> clazz) {
    return _commandNameLookup.get(clazz);
  }
}
