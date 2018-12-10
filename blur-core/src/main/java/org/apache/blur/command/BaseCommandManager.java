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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.blur.command.annotation.Description;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.BlurException;
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

public abstract class BaseCommandManager implements Closeable {

  private static final String META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS = "META-INF/services/org.apache.blur.command.Commands";
  private static final Log LOG = LogFactory.getLog(BaseCommandManager.class);

  private final ExecutorService _executorServiceWorker;
  private final ExecutorService _executorServiceDriver;
  private final Random _random = new Random();

  protected final Map<String, BigInteger> _commandLoadTime = new ConcurrentHashMap<String, BigInteger>();
  protected final Map<String, Command<?>> _command = new ConcurrentHashMap<String, Command<?>>();
  protected final Map<Class<? extends Command<?>>, String> _commandNameLookup = new ConcurrentHashMap<Class<? extends Command<?>>, String>();
  protected final ConcurrentMap<Long, ResponseFuture<?>> _driverRunningMap = new MapMaker().makeMap();
  protected final ConcurrentMap<Long, ResponseFuture<?>> _workerRunningMap = new MapMaker().makeMap();
  protected final long _connectionTimeout;
  protected final File _tmpPath;
  protected final String _commandPath;
  protected final Timer _timer;
  protected final long _pollingPeriod = TimeUnit.SECONDS.toMillis(15);
  protected final Map<Path, BigInteger> _commandPathLastChange = new ConcurrentHashMap<Path, BigInteger>();
  protected final Configuration _configuration;
  protected final BlurObjectSerDe _serDe = new BlurObjectSerDe();
  protected final long _runningCacheTombstoneTime = TimeUnit.SECONDS.toMillis(60);

  public BaseCommandManager(File tmpPath, String commandPath, int workerThreadCount, int driverThreadCount,
      long connectionTimeout, Configuration configuration) throws IOException {
    _configuration = configuration;
    lookForCommandsToRegisterInClassPath();
    _tmpPath = tmpPath;
    _commandPath = commandPath;
    _executorServiceWorker = Executors.newThreadPool("command-worker-", workerThreadCount);
    _executorServiceDriver = Executors.newThreadPool("command-driver-", driverThreadCount);
    _connectionTimeout = connectionTimeout / 2;
    _timer = new Timer("BaseCommandManager-Timer", true);
    _timer.schedule(getTimerTaskForRemovalOfOldCommands(_driverRunningMap), _pollingPeriod, _pollingPeriod);
    _timer.schedule(getTimerTaskForRemovalOfOldCommands(_workerRunningMap), _pollingPeriod, _pollingPeriod);
    if (_tmpPath == null || _commandPath == null) {
      LOG.info("Tmp Path [{0}] or Command Path [{1}] is null so the automatic command reload will be disabled.",
          _tmpPath, _commandPath);
    } else {
      loadNewCommandsFromCommandPath();
      _timer.schedule(getNewCommandTimerTask(), _pollingPeriod, _pollingPeriod);
    }
  }

  public void cancelCommand(String commandExecutionId) {
    LOG.info("Trying to cancel command [{0}]", commandExecutionId);
    cancelAllExecuting(commandExecutionId, _workerRunningMap);
    cancelAllExecuting(commandExecutionId, _driverRunningMap);
  }

  private void cancelAllExecuting(String commandExecutionId, ConcurrentMap<Long, ResponseFuture<?>> runningMap) {
    for (Entry<Long, ResponseFuture<?>> e : runningMap.entrySet()) {
      Long instanceExecutionId = e.getKey();
      ResponseFuture<?> future = e.getValue();
      Command<?> commandExecuting = future.getCommandExecuting();
      if (commandExecuting.getCommandExecutionId().equals(commandExecutionId)) {
        LOG.info("Canceling Command with executing id [{0}] command [{1}]", instanceExecutionId, commandExecuting);
        future.cancel(true);
      }
    }
  }

  public List<String> commandStatusList(CommandStatusStateEnum commandStatus) {
    throw new RuntimeException("Not implemented.");
  }

  private TimerTask getTimerTaskForRemovalOfOldCommands(final Map<Long, ResponseFuture<?>> runningMap) {
    return new TimerTask() {
      @Override
      public void run() {
        Set<Entry<Long, ResponseFuture<?>>> entrySet = runningMap.entrySet();
        for (Entry<Long, ResponseFuture<?>> e : entrySet) {
          Long instanceExecutionId = e.getKey();
          ResponseFuture<?> responseFuture = e.getValue();
          if (!responseFuture.isRunning() && responseFuture.hasExpired()) {
            Command<?> commandExecuting = responseFuture.getCommandExecuting();
            String commandExecutionId = null;
            if (commandExecuting != null) {
              commandExecutionId = commandExecuting.getCommandExecutionId();
            }
            LOG.info("Removing old execution instance id [{0}] with command execution id of [{1}]",
                instanceExecutionId, commandExecutionId);
            runningMap.remove(instanceExecutionId);
          }
        }
      }
    };
  }

  public Map<String, BigInteger> getCommands() {
    return new HashMap<String, BigInteger>(_commandLoadTime);
  }

  public Set<Argument> getRequiredArguments(String commandName) {
    Command<?> command = getCommandObject(commandName, null);
    return command.getRequiredArguments();
  }

  public Set<Argument> getOptionalArguments(String commandName) {
    Command<?> command = getCommandObject(commandName, null);
    return command.getOptionalArguments();
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

  public int commandRefresh() throws IOException {
    return loadNewCommandsFromCommandPath();
  }

  protected synchronized int loadNewCommandsFromCommandPath() throws IOException {
    Path path = new Path(_commandPath);
    FileSystem fileSystem = path.getFileSystem(_configuration);
    int changeCount = 0;
    if(fileSystem.exists(path)) {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        BigInteger contentsCheck = checkContents(fileStatus, fileSystem);
        Path entryPath = fileStatus.getPath();
        BigInteger currentValue = _commandPathLastChange.get(entryPath);
        if (!contentsCheck.equals(currentValue)) {
          changeCount++;
          loadNewCommand(fileSystem, fileStatus, contentsCheck);
          _commandPathLastChange.put(entryPath, contentsCheck);
        }
      }
    }
    return changeCount;
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
    copyLocal(fileSystem, fileStatus, file);
    URLClassLoader loader = new URLClassLoader(getUrls(file).toArray(new URL[] {}));
    Enumeration<URL> resources = loader.getResources(META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS);
    loadCommandClasses(resources, loader, hashOfContents);
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

  protected void copyLocal(FileSystem fileSystem, FileStatus fileStatus, File destDir) throws IOException {
    Path path = fileStatus.getPath();
    File file = new File(destDir, path.getName());
    if (fileStatus.isDir()) {
      if (!file.mkdirs()) {
        LOG.error("Error while trying to create a sub directory [{0}].", file.getAbsolutePath());
        throw new IOException("Error while trying to create a sub directory [" + file.getAbsolutePath() + "].");
      }
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fs : listStatus) {
        copyLocal(fileSystem, fs, file);
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
      LOG.debug("Scanning directory [{0}].", fileStatus.getPath());
      BigInteger count = BigInteger.ZERO;
      Path path = fileStatus.getPath();
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fs : listStatus) {
        count = count.add(checkContents(fs, fileSystem));
      }
      return count;
    } else {
      int hashCode = fileStatus.getPath().toString().hashCode();
      long modificationTime = fileStatus.getModificationTime();
      long len = fileStatus.getLen();
      BigInteger bi = BigInteger.valueOf(hashCode).add(
          BigInteger.valueOf(modificationTime).add(BigInteger.valueOf(len)));
      LOG.debug("File path hashcode [{0}], mod time [{1}], len [{2}] equals file code [{3}].",
          Integer.toString(hashCode), Long.toString(modificationTime), Long.toString(len),
          bi.toString(Character.MAX_RADIX));
      return bi;
    }
  }

  protected void lookForCommandsToRegisterInClassPath() throws IOException {
    Enumeration<URL> systemResources = ClassLoader
        .getSystemResources(META_INF_SERVICES_ORG_APACHE_BLUR_COMMAND_COMMANDS);
    loadCommandClasses(systemResources, getClass().getClassLoader(), BigInteger.ZERO);
  }

  @SuppressWarnings("unchecked")
  protected void loadCommandClasses(Enumeration<URL> enumeration, ClassLoader loader, BigInteger version)
      throws IOException {
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
        register((Class<? extends Command<?>>) loader.loadClass(classNameToRegister), version);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public Response reconnect(Long instanceExecutionId) throws IOException, TimeoutException {
    Future<Response> future = (Future<Response>) _driverRunningMap.get(instanceExecutionId);
    if (future == null) {
      throw new IOException("Execution instance id [" + instanceExecutionId + "] did not find any executing commands.");
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
      LOG.info("Timeout of command [{0}]", instanceExecutionId);
      throw new TimeoutException(instanceExecutionId);
    }
  }

  protected Response submitDriverCallable(Callable<Response> callable, Command<?> commandExecuting) throws IOException,
      TimeoutException, ExceptionCollector {
    Future<Response> future = _executorServiceDriver.submit(callable);
    Long instanceExecutionId = getInstanceExecutionId();
    _driverRunningMap.put(instanceExecutionId, new ResponseFuture<Response>(_runningCacheTombstoneTime, future,
        commandExecuting));
    try {
      return future.get(_connectionTimeout, TimeUnit.MILLISECONDS);
    } catch (CancellationException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ExceptionCollector) {
        throw (ExceptionCollector) cause;
      }
      throw new IOException(cause);
    } catch (java.util.concurrent.TimeoutException e) {
      LOG.info("Timeout of command [{0}]", instanceExecutionId);
      throw new TimeoutException(instanceExecutionId);
    }
  }

  private Long getInstanceExecutionId() {
    synchronized (_random) {
      while (true) {
        Long id = _random.nextLong();
        if (_driverRunningMap.containsKey(id)) {
          continue;
        }
        if (_workerRunningMap.containsKey(id)) {
          continue;
        }
        return id;
      }
    }
  }

  protected <T> Future<T> submitToExecutorService(Callable<T> callable, Command<?> commandExecuting) {
    Future<T> future = _executorServiceWorker.submit(callable);
    Long instanceExecutionId = getInstanceExecutionId();
    _workerRunningMap.put(instanceExecutionId, new ResponseFuture<T>(_runningCacheTombstoneTime, future,
        commandExecuting));
    return future;
  }

  @Override
  public void close() throws IOException {
    _executorServiceWorker.shutdownNow();
    _executorServiceDriver.shutdownNow();
    if (_timer != null) {
      _timer.cancel();
      _timer.purge();
    }
  }

  public void register(Class<? extends Command<?>> commandClass, BigInteger version) throws IOException {
    try {
      Command<?> command = commandClass.newInstance();
      _command.put(command.getName(), command);
      _commandLoadTime.put(command.getName(), version);
      _commandNameLookup.put(commandClass, command.getName());
      LOG.info("Command [{0}] from class [{1}] registered.", command.getName(), commandClass.getName());
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  protected Command<?> getCommandObject(String commandName, ArgumentOverlay argumentOverlay) {
    Command<?> command = _command.get(commandName);
    if (command == null) {
      return null;
    }
    Command<?> clone = command.clone();
    if (argumentOverlay == null) {
      return clone;
    }
    return argumentOverlay.setup(clone);
  }

  protected String getCommandName(Class<? extends Command<?>> clazz) {
    return _commandNameLookup.get(clazz);
  }

  // protected Map<String, Set<Shard>> getShards(TableContextFactory
  // tableContextFactory, Command<?> command,
  // final Args args, Set<String> tables) throws IOException {
  // Map<String, Set<Shard>> shardMap = new TreeMap<String, Set<Shard>>();
  // if (command instanceof ShardRoute) {
  // ShardRoute shardRoute = (ShardRoute) command;
  // for (String table : tables) {
  // shardMap.put(table,
  // shardRoute.resolveShards(tableContextFactory.getTableContext(table),
  // args));
  // }
  // } else {
  // if (tables.size() > 1) {
  // throw new IOException(
  // "Cannot route to single shard when multiple tables are specified.  Implement ShardRoute on your command.");
  // }
  // String singleTable = tables.iterator().next();
  // Set<Shard> shardSet = new TreeSet<Shard>();
  // String shard = args.get("shard");
  // if (shard == null) {
  // BlurArray shardArray = args.get("shards");
  // if (shardArray != null) {
  // for (int i = 0; i < shardArray.length(); i++) {
  // shardSet.add(new Shard(singleTable, shardArray.getString(i)));
  // }
  // }
  // } else {
  // shardSet.add(new Shard(singleTable, shard));
  // }
  // shardMap.put(singleTable, shardSet);
  // }
  // return shardMap;
  // }

  // protected Set<String> getTables(Command<?> command, final Args args) throws
  // IOException {
  // Set<String> tables = new TreeSet<String>();
  // if (command instanceof TableRoute) {
  // TableRoute tableRoute = (TableRoute) command;
  // tables.addAll(tableRoute.resolveTables(args));
  // } else {
  // if (args == null) {
  // return tables;
  // }
  // String table = args.get("table");
  // if (table == null) {
  // BlurArray tableArray = args.get("tables");
  // if (tableArray == null) {
  // return tables;
  // }
  // for (int i = 0; i < tableArray.length(); i++) {
  // tables.add(tableArray.getString(i));
  // }
  // } else {
  // tables.add(table);
  // }
  // }
  // return tables;
  // }

  @SuppressWarnings("unchecked")
  public String getDescription(String commandName) {
    Command<?> command = getCommandObject(commandName, null);
    if (command == null) {
      return null;
    }
    Class<? extends Command<?>> clazz = (Class<? extends Command<?>>) command.getClass();
    Description description = clazz.getAnnotation(Description.class);
    if (description == null) {
      return null;
    }
    return description.value();
  }

  public String getReturnType(String commandName) {
    Command<?> command = getCommandObject(commandName, null);
    if (command == null) {
      return null;
    }
    return command.getReturnType();
  }

  protected Arguments toArguments(Command<?> command) throws IOException {
    try {
      return CommandUtil.toArguments(command, _serDe);
    } catch (BlurException e) {
      throw new IOException(e);
    }
  }

  protected void validate(Command<?> command) throws IOException {
    String name = command.getName();
    Command<?> cmd = getCommandObject(name, null);
    if (cmd == null) {
      throw new IOException("Command [" + name + "] not found.");
    }
    if (cmd.getClass().equals(command.getClass())) {
      return;
    }
    throw new IOException("Command [" + name + "] class does not match class registered.");
  }

}
