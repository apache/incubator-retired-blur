package org.apache.blur.command;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.google.common.collect.MapMaker;

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

  private static final String META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS = "META-INF/services/org.apache.blur.command.Commands";
  private static final Log LOG = LogFactory.getLog(BaseCommandManager.class);

  private final ExecutorService _executorService;
  private final ExecutorService _executorServiceDriver;

  protected final Map<String, Command> _command = new ConcurrentHashMap<String, Command>();
  protected final Map<Class<? extends Command>, String> _commandNameLookup = new ConcurrentHashMap<Class<? extends Command>, String>();
  protected final ConcurrentMap<ExecutionId, Future<Response>> _runningMap;
  protected final long _connectionTimeout;

  public BaseCommandManager(int threadCount, long connectionTimeout) throws IOException {
    lookForCommandsToRegister();
    _executorService = Executors.newThreadPool("command-", threadCount);
    _executorServiceDriver = Executors.newThreadPool("command-driver-", threadCount);
    _connectionTimeout = connectionTimeout / 2;
    _runningMap = new MapMaker().weakKeys().makeMap();
  }

  @SuppressWarnings("unchecked")
  private void lookForCommandsToRegister() throws IOException {
    Enumeration<URL> systemResources = ClassLoader
        .getSystemResources(META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS);
    Properties properties = new Properties();
    while (systemResources.hasMoreElements()) {
      URL url = systemResources.nextElement();
      InputStream inputStream = url.openStream();
      properties.load(inputStream);
      inputStream.close();
    }
    Set<Object> keySet = properties.keySet();
    for (Object o : keySet) {
      String classNameToRegister = o.toString();
      try {
        register((Class<? extends Command>) Class.forName(classNameToRegister));
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }

  public Response reconnect(ExecutionId executionId) throws IOException, TimeoutException {
    Future<Response> future = _runningMap.get(executionId);
    if (future == null) {
      throw new IOException("Command id [" + executionId + "] did not find any executing commands.");
    }
    try {
      return future.get(_connectionTimeout, TimeUnit.MILLISECONDS);
    } catch (CancellationException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (java.util.concurrent.TimeoutException e) {
      LOG.info("Timeout of command [{0}]", executionId);
      throw new TimeoutException(executionId);
    }
  }

  protected Response submitDriverCallable(Callable<Response> callable) throws IOException, TimeoutException {
    ExecutionContext executionContext = ExecutionContext.create();
    Future<Response> future = _executorServiceDriver.submit(executionContext.wrapCallable(callable));
    executionContext.registerDriverFuture(future);
    ExecutionId executionId = executionContext.getExecutionId();
    _runningMap.put(executionId, future);
    try {
      return future.get(_connectionTimeout, TimeUnit.MILLISECONDS);
    } catch (CancellationException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } catch (java.util.concurrent.TimeoutException e) {
      LOG.info("Timeout of command [{0}]", executionId);
      throw new TimeoutException(executionId);
    }
  }

  protected <T> Future<T> submitToExecutorService(Callable<T> callable) {
    ExecutionContext executionContext = ExecutionContext.get();
    Future<T> future = _executorService.submit(executionContext.wrapCallable(callable));
    executionContext.registerFuture(future);
    return future;
  }

  @Override
  public void close() throws IOException {
    _executorService.shutdownNow();
    _executorServiceDriver.shutdownNow();
  }

  public void register(Class<? extends Command> commandClass) throws IOException {
    try {
      Command command = commandClass.newInstance();
      _command.put(command.getName(), command);
      _commandNameLookup.put(commandClass, command.getName());
      LOG.info("Command [{0}] from class [{1}] registered.", command.getName(), commandClass.getName());
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  protected Command getCommandObject(String commandName) {
    return _command.get(commandName);
  }

  protected String getCommandName(Class<? extends Command> clazz) {
    return _commandNameLookup.get(clazz);
  }

  protected Map<String, Set<Shard>> getShards(Command command, final Args args, Set<String> tables) throws IOException {
    Map<String, Set<Shard>> shardMap = new TreeMap<String, Set<Shard>>();
    if (command instanceof ShardRoute) {
      ShardRoute shardRoute = (ShardRoute) command;
      for (String table : tables) {
        shardMap.put(table, shardRoute.resolveShards(table, args));
      }
    } else {
      if (tables.size() > 1) {
        throw new IOException(
            "Cannot route to single shard when multiple tables are specified.  Implement ShardRoute on your command.");
      }
      String singleTable = tables.iterator().next();
      Set<Shard> shardSet = new TreeSet<Shard>();
      String shard = args.get("shard");
      if (shard == null) {
        BlurArray shardArray = args.get("shards");
        if (shardArray != null) {
          for (int i = 0; i < shardArray.length(); i++) {
            shardSet.add(new Shard(singleTable, shardArray.getString(i)));
          }
        }
      } else {
        shardSet.add(new Shard(singleTable, shard));
      }
      shardMap.put(singleTable, shardSet);
    }
    return shardMap;
  }

  protected Set<String> getTables(Command command, final Args args) throws IOException {
    Set<String> tables = new TreeSet<String>();
    if (command instanceof TableRoute) {
      TableRoute tableRoute = (TableRoute) command;
      tables.addAll(tableRoute.resolveTables(args));
    } else {
      if (args == null) {
        return tables;
      }
      String table = args.get("table");
      if (table == null) {
        BlurArray tableArray = args.get("tables");
        if (tableArray == null) {
          return tables;
        }
        for (int i = 0; i < tableArray.length(); i++) {
          tables.add(tableArray.getString(i));
        }
      } else {
        tables.add(table);
      }
    }
    return tables;
  }
}
