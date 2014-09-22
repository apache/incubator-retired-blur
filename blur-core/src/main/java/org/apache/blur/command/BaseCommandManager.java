package org.apache.blur.command;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
  protected final String _tmpPath;
  protected final String _commandPath;
  protected final Timer _timer;
  protected final long _pollingPeriod = TimeUnit.SECONDS.toMillis(15);
  protected final Map<Path, BigInteger> _commandLastChange = new ConcurrentHashMap<Path, BigInteger>();
  protected final Configuration _configuration;

  public BaseCommandManager(String tmpPath, String commandPath, int workerThreadCount, int driverThreadCount,
      long connectionTimeout, Configuration configuration) throws IOException {
    _configuration = configuration;
    lookForCommandsToRegisterInClassPath();
    _tmpPath = tmpPath;
    _commandPath = commandPath;
    _executorService = Executors.newThreadPool("command-worker-", workerThreadCount);
    _executorServiceDriver = Executors.newThreadPool("command-driver-", driverThreadCount);
    _connectionTimeout = connectionTimeout / 2;
    _runningMap = new MapMaker().weakKeys().makeMap();
    if (_tmpPath == null || _commandPath == null) {
      _timer = null;
      LOG.info("Tmp Path [{0}] or Command Path [{1}] is null so the automatic command reload will be disabled.",
          _tmpPath, _commandPath);
    } else {
      loadNewCommandsFromCommandPath();
      _timer = new Timer("Command-Loader", true);
      _timer.schedule(getNewCommandTimerTask(), _pollingPeriod, _pollingPeriod);
    }
  }

  protected TimerTask getNewCommandTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          loadNewCommandsFromCommandPath();
        } catch (Throwable t) {
          LOG.error("Unknown error while trying to load new commands.", t);
        }
      }
    };
  }

  public void commandRefresh() throws IOException {
    loadNewCommandsFromCommandPath();
  }

  protected synchronized void loadNewCommandsFromCommandPath() throws IOException {
    Path path = new Path(_commandPath);
    FileSystem fileSystem = path.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(path);
    for (FileStatus fileStatus : listStatus) {
      BigInteger contentsCheck = checkContents(fileStatus, fileSystem);
      Path entryPath = fileStatus.getPath();
      BigInteger currentValue = _commandLastChange.get(entryPath);
      if (!contentsCheck.equals(currentValue)) {
        loadNewCommand(fileSystem, fileStatus, contentsCheck);
        _commandLastChange.put(entryPath, contentsCheck);
      }
    }
  }

  protected void loadNewCommand(FileSystem fileSystem, FileStatus fileStatus, BigInteger hashOfContents)
      throws IOException {
    File file = new File(_tmpPath, UUID.randomUUID().toString());
    if (!file.mkdirs()) {
      LOG.error("Error while trying to create a tmp directory for loading a new command set from [{0}].",
          fileStatus.getPath());
      return;
    }
    LOG.info("Copying new command with hash [{2}] set from [{0}] into [{1}].", fileStatus.getPath(),
        file.getAbsolutePath(), hashOfContents.toString(Character.MAX_RADIX));
    copyLocal(fileSystem, fileStatus.getPath(), file);
    URLClassLoader loader = new URLClassLoader(getUrls(file).toArray(new URL[] {}));
    Enumeration<URL> resources = loader.getResources(META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS);
    loadCommandClasses(resources, loader);
  }

  protected List<URL> getUrls(File file) throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        urls.addAll(getUrls(f));
      }
    } else {
      URL url = file.toURI().toURL();
      LOG.info("Adding url [{0}] to be loaded.", url);
      urls.add(url);
    }
    return urls;
  }

  protected void copyLocal(FileSystem fileSystem, Path path, File destDir) throws IOException {
    File file = new File(destDir, path.getName());
    if (fileSystem.isDirectory(path)) {
      if (!file.mkdirs()) {
        LOG.error("Error while trying to create a sub directory [{0}].", file.getAbsolutePath());
        throw new IOException("Error while trying to create a sub directory [" + file.getAbsolutePath() + "].");
      }
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        copyLocal(fileSystem, fileStatus.getPath(), file);
      }
    } else {
      FileOutputStream output = new FileOutputStream(file);
      FSDataInputStream inputStream = fileSystem.open(path);
      IOUtils.copy(inputStream, output);
      inputStream.close();
      output.close();
    }
  }

  protected BigInteger checkContents(FileStatus fileStatus, FileSystem fileSystem) throws IOException {
    if (fileStatus.isDir()) {
      BigInteger count = BigInteger.ZERO;
      Path path = fileStatus.getPath();
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fs : listStatus) {
        count = count.add(checkContents(fs, fileSystem));
      }
      return count;
    } else {
      return BigInteger.valueOf(fileStatus.getModificationTime());
    }
  }

  protected void lookForCommandsToRegisterInClassPath() throws IOException {
    Enumeration<URL> systemResources = ClassLoader
        .getSystemResources(META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS);
    loadCommandClasses(systemResources, getClass().getClassLoader());
  }

  @SuppressWarnings("unchecked")
  protected void loadCommandClasses(Enumeration<URL> enumeration, ClassLoader loader) throws IOException {
    Properties properties = new Properties();
    while (enumeration.hasMoreElements()) {
      URL url = enumeration.nextElement();
      InputStream inputStream = url.openStream();
      properties.load(inputStream);
      inputStream.close();
    }
    Set<Object> keySet = properties.keySet();
    for (Object o : keySet) {
      String classNameToRegister = o.toString();
      LOG.info("Loading class [{0}]", classNameToRegister);
      try {
        register((Class<? extends Command>) loader.loadClass(classNameToRegister));
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
    if (_timer != null) {
      _timer.cancel();
      _timer.purge();
    }
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
