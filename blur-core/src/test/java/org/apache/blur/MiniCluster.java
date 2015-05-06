package org.apache.blur;

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

import static org.apache.blur.utils.BlurConstants.BLUR_CONTROLLER_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_CONTROLLER_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_GUI_SHARD_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_SLAB_COUNT;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_SAFEMODEDELAY;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.ThriftBlurControllerServer;
import org.apache.blur.thrift.ThriftBlurShardServer;
import org.apache.blur.thrift.ThriftServer;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.MemoryReporter;
import org.apache.blur.zookeeper.ZkMiniCluster;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MiniCluster {

  private static Log LOG = LogFactory.getLog(MiniCluster.class);
  private MiniDFSCluster cluster;
  private final String id = UUID.randomUUID().toString();
  private ZkMiniCluster zkMiniCluster = new ZkMiniCluster();
  private List<MiniClusterServer> controllers = new ArrayList<MiniClusterServer>();
  private List<MiniClusterServer> shards = new ArrayList<MiniClusterServer>();
  private ThreadGroup group = new ThreadGroup(id);
  private Configuration _conf;
  private Object mrMiniCluster;
  private Configuration _mrConf;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException, BlurException,
      TException {
    MiniCluster miniCluster = new MiniCluster();
    miniCluster.startDfs("./tmp/hdfs");
    miniCluster.startZooKeeper("./tmp/zk");
    miniCluster.startControllers(1, false, false);
    miniCluster.startShards(1, false, false);

//    try {
      Iface client = BlurClient.getClient(miniCluster.getControllerConnectionStr());
      miniCluster.createTable("test", client);
      long start = System.nanoTime();
      for (int i = 0; i < 1000; i++) {
        long now = System.nanoTime();
        if (start + 5000000000L < now) {
          System.out.println("Total [" + i + "]");
          start = now;
        }
        miniCluster.addRow("test", i, client);
      }

      // This waits for all the data to become visible.
      Thread.sleep(2000);

      for (int i = 0; i < 1000; i++) {
        miniCluster.searchRow("test", i, client);
      }

//    } finally {
//      miniCluster.stopShards();
//      miniCluster.stopControllers();
//      miniCluster.shutdownZooKeeper();
//      miniCluster.shutdownDfs();
//    }
  }

  public void startBlurCluster(String path, int controllerCount, int shardCount) {
    startBlurCluster(path, controllerCount, shardCount, false, false);
  }

  public void startBlurCluster(String path, int controllerCount, int shardCount, boolean randomPort) {
    startBlurCluster(path, controllerCount, shardCount, randomPort, false);
  }

  public void startBlurCluster(final String path, final int controllerCount, final int shardCount,
      final boolean randomPort, final boolean externalProcesses) {
    Thread thread = new Thread(group, new Runnable() {
      @Override
      public void run() {
        MemoryReporter.enable();
        startDfs(path + "/hdfs");
        startZooKeeper(path + "/zk", randomPort);
        setupBuffers();
        startControllers(controllerCount, randomPort, externalProcesses);
        startShards(shardCount, randomPort, externalProcesses);
        try {
          waitForSafeModeToExit();
        } catch (BlurException e) {
          throw new RuntimeException(e);
        } catch (TException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void stopMrMiniCluster() throws IOException {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("js");
    if (useYarn()) {
      engine.put("mrMiniCluster", mrMiniCluster);
      try {
        engine.eval("mrMiniCluster.stop();");
      } catch (ScriptException e) {
        throw new IOException(e);
      }
    } else {
      engine.put("mrMiniCluster", mrMiniCluster);
      try {
        engine.eval("mrMiniCluster.shutdown();");
      } catch (ScriptException e) {
        throw new IOException(e);
      }
    }
  }

  public void startMrMiniCluster() throws IOException {
    _mrConf = startMrMiniClusterInternal(getFileSystemUri().toString());
  }

  public void startMrMiniCluster(String fileSystemUri) throws IOException {
    _mrConf = startMrMiniClusterInternal(fileSystemUri);
  }

  public Configuration getMRConfiguration() {
    return _mrConf;
  }

  private Configuration startMrMiniClusterInternal(String fileSystemUri) throws IOException {
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("js");

    if (useYarn()) {
      int nodeManagers = 1;
      Class<?> c = getClass();
      engine.put("c", c);
      engine.put("nodeManagers", nodeManagers);
      engine.put("fileSystemUri", fileSystemUri);
      try {
        engine.eval("conf = new org.apache.hadoop.yarn.conf.YarnConfiguration()");
        engine.eval("org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, fileSystemUri);");
        engine
            .eval("mrMiniCluster = org.apache.hadoop.mapred.MiniMRClientClusterFactory.create(c, nodeManagers, conf);");
        engine.eval("mrMiniCluster.start();");
        engine.eval("configuration = mrMiniCluster.getConfig();");
      } catch (ScriptException e) {
        throw new IOException(e);
      }

      Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
      mrMiniCluster = bindings.get("mrMiniCluster");
      return (Configuration) bindings.get("configuration");
    } else {
      int numTaskTrackers = 1;
      int numDir = 1;
      engine.put("fileSystemUri", fileSystemUri);
      engine.put("numTaskTrackers", numTaskTrackers);
      engine.put("numDir", numDir);

      try {
        engine
            .eval("mrMiniCluster = new org.apache.hadoop.mapred.MiniMRCluster(numTaskTrackers, fileSystemUri, numDir);");
        engine.eval("configuration = mrMiniCluster.createJobConf();");
      } catch (ScriptException e) {
        throw new IOException(e);
      }
      Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
      mrMiniCluster = bindings.get("mrMiniCluster");
      return (Configuration) bindings.get("configuration");
    }
  }

  private boolean useYarn() {
    String version = VersionInfo.getVersion();
    if (version.startsWith("0.20.") || version.startsWith("1.")) {
      return false;
    }
    // Check for mr1 hadoop2
    if (isMr1Hadoop2()) {
      return false;
    }
    return true;
  }

  private boolean isMr1Hadoop2() {
    try {
      Enumeration<URL> e = ClassLoader.getSystemClassLoader().getResources(
          "META-INF/maven/org.apache.hadoop/hadoop-client/pom.properties");
      while (e.hasMoreElements()) {
        URL url = e.nextElement();
        InputStream stream = url.openStream();
        Properties properties = new Properties();
        properties.load(stream);
        Object object = properties.get("version");
        if (object.toString().contains("mr1")) {
          return true;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  private void waitForSafeModeToExit() throws BlurException, TException, IOException {
    String controllerConnectionStr = getControllerConnectionStr();
    Iface client = BlurClient.getClient(controllerConnectionStr);
    String clusterName = "default";
    boolean inSafeMode;
    boolean isNoLonger = false;
    do {
      inSafeMode = client.isInSafeMode(clusterName);
      if (!inSafeMode) {
        if (isNoLonger) {
          System.out.println("Cluster " + cluster + " is no longer in safemode.");
        } else {
          System.out.println("Cluster " + cluster + " is not in safemode.");
        }
        return;
      }
      isNoLonger = true;
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } while (inSafeMode);
  }

  private void setupBuffers() {
    BufferStore.initNewBuffer(128, 128 * 128);
  }

  public void shutdownBlurCluster() {
    stopShards();
    stopControllers();
    shutdownZooKeeper();
    shutdownDfs();
  }

  private void createTable(String test, Iface client) throws BlurException, TException, IOException {
    final TableDescriptor descriptor = new TableDescriptor();
    descriptor.setName(test);
    descriptor.setShardCount(7);
    descriptor.setTableUri(getFileSystemUri() + "/blur/" + test);
    client.createTable(descriptor);
  }

  public String getControllerConnectionStr() {
    String zkConnectionString = zkMiniCluster.getZkConnectionString();
    ZooKeeper zk;
    try {
      zk = new ZooKeeperClient(zkConnectionString, 30000, new Watcher() {
        @Override
        public void process(WatchedEvent event) {

        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String onlineControllersPath = ZookeeperPathConstants.getOnlineControllersPath();
    try {
      List<String> children = zk.getChildren(onlineControllersPath, false);
      StringBuilder builder = new StringBuilder();
      for (String s : children) {
        if (builder.length() != 0) {
          builder.append(',');
        }
        builder.append(s);
      }
      return builder.toString();
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        zk.close();
      } catch (InterruptedException e) {
        LOG.error("Unkown error while trying to close ZooKeeper client.", e);
      }
    }
  }

  private void addRow(String table, int i, Iface client) throws BlurException, TException {
    Row row = new Row();
    row.setId(Integer.toString(i));
    Record record = new Record();
    record.setRecordId(Integer.toString(i));
    record.setFamily("test");
    record.addToColumns(new Column("test", Integer.toString(i)));
    row.addToRecords(record);
    RowMutation rowMutation = BlurUtil.toRowMutation(table, row);
    client.mutate(rowMutation);
  }

  private void searchRow(String table, int i, Iface client) throws BlurException, TException {
    BlurQuery blurQuery = BlurThriftHelper.newSimpleQuery("test.test:" + i);
    System.out.println("Running [" + blurQuery + "]");
    BlurResults results = client.query(table, blurQuery);
    if (results.getTotalResults() != 1L) {
      throw new RuntimeException("we got a problem here.");
    }
  }

  public void stopControllers() {
    IOUtils.cleanup(LOG, controllers.toArray(new Closeable[] {}));
  }

  public void stopShards() {
    IOUtils.cleanup(LOG, shards.toArray(new Closeable[] {}));
  }

  public void startControllers(int num, boolean randomPort, boolean externalProcesses) {
    BlurConfiguration configuration = getBlurConfiguration();
    startControllers(configuration, num, randomPort, externalProcesses);
  }

  private BlurConfiguration getBlurConfiguration(BlurConfiguration overrides) {
    BlurConfiguration conf = getBlurConfiguration();

    for (Map.Entry<String, String> over : overrides.getProperties().entrySet()) {
      conf.set(over.getKey().toString(), over.getValue().toString());
    }
    return conf;
  }

  private BlurConfiguration getBlurConfiguration() {
    BlurConfiguration configuration;
    try {
      configuration = new BlurConfiguration();
    } catch (IOException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    configuration.set(BLUR_ZOOKEEPER_CONNECTION, getZkConnectionString());
    configuration.set(BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION, "false");
    configuration.set(BLUR_SHARD_BLOCKCACHE_SLAB_COUNT, "0");
    configuration.setLong(BLUR_SHARD_SAFEMODEDELAY, 5000);
    configuration.setInt(BLUR_GUI_CONTROLLER_PORT, -1);
    configuration.setInt(BLUR_GUI_SHARD_PORT, -1);

    return configuration;
  }

  public void startControllers(BlurConfiguration configuration, int num, boolean randomPort, boolean externalProcesses) {
    BlurConfiguration localConf = getBlurConfiguration(configuration);
    if (randomPort) {
      localConf.setInt(BLUR_CONTROLLER_BIND_PORT, 0);
    }
    for (int i = 0; i < num; i++) {
      try {
        MiniClusterServer miniClusterServer = toMiniClusterServer(i, TYPE.controller, localConf, externalProcesses);
        controllers.add(miniClusterServer);
        startServer(miniClusterServer);
      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  enum TYPE {
    shard, controller
  }

  private MiniClusterServer toMiniClusterServer(int serverIndex, TYPE type, final BlurConfiguration configuration,
      boolean externalProcesses) {
    if (externalProcesses) {
      return toMiniClusterServerExternal(serverIndex, type, configuration);
    } else {
      return toMiniClusterServerInternal(serverIndex, type, configuration);
    }
  }

  private MiniClusterServer toMiniClusterServerExternal(final int serverIndex, final TYPE type,
      BlurConfiguration configuration) {
    // write out configuration to tmp file.
    // build args
    // build class path
    try {
      File dir = new File("./target/blurtesttemp");
      dir.mkdirs();
      final File file = new File(dir, "blurtestconf." + UUID.randomUUID().toString() + ".properties").getAbsoluteFile();
      OutputStream outputStream = new FileOutputStream(file);
      configuration.write(outputStream);
      outputStream.close();

      System.out.println("File [" + file.getAbsolutePath() + "] exists [" + file.exists() + "]");

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          file.delete();
        }
      }));

      String javaHome = System.getProperty("java.home");
      String classPath = System.getProperty("java.class.path");

      List<String> command = new ArrayList<String>();
      command.add(javaHome + "/bin/java");
      command.add("-Xmx512m");
      command.add("-Xms512m");
      command.add("-cp");
      command.add(classPath);
      command.add(ExternalThriftServer.class.getName());
      command.add(type.name());
      command.add(Integer.toString(serverIndex));
      command.add(file.getAbsolutePath());
      final ProcessBuilder builder = new ProcessBuilder(command);

      final MiniClusterServer miniClusterServer = new MiniClusterServer() {

        private Process _process;
        private AtomicBoolean _online = new AtomicBoolean();

        @Override
        public void close() throws IOException {
          kill();
        }

        @Override
        public void waitUntilOnline() {
          while (!_online.get()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              LOG.error("Unknown error while trying to wait for process to come online.", e);
            }
          }
        }

        @Override
        public void start() {
          try {
            _process = builder.start();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          pipeOutputToStdOut(_process.getInputStream());
          pipeOutputToStdOut(_process.getErrorStream());
        }

        private void pipeOutputToStdOut(final InputStream inputStream) {
          new Thread(new Runnable() {
            @Override
            public void run() {
              BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
              String line;
              try {
                while ((line = reader.readLine()) != null) {
                  LOG.info("Process Output Type [" + type + "] Index [" + serverIndex + "] Line [" + line + "]");
                  if (line.trim().equals("ONLINE")) {
                    _online.set(true);
                  }
                }
              } catch (IOException e) {
                LOG.error("Unknown error while trying to follow input stream.", e);
              }
            }
          }).start();
        }

        @Override
        public void kill() {
          if (_process != null) {
            _process.destroy();
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          miniClusterServer.kill();
        }
      }));
      return miniClusterServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private MiniClusterServer toMiniClusterServerInternal(int serverIndex, TYPE type,
      final BlurConfiguration configuration) {
    final ThriftServer thriftServer;
    try {
      thriftServer = getThriftServer(serverIndex, type, configuration);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new MiniClusterServer() {

      @Override
      public void close() throws IOException {
        thriftServer.close();
      }

      @Override
      public void kill() {
        ZooKeeper zk = null;
        try {
          int shardPort = ThriftServer.getBindingPort(thriftServer.getServerTransport());
          String nodeNameHostname = ThriftServer.getNodeName(configuration, BLUR_SHARD_HOSTNAME);
          String nodeName = nodeNameHostname + ":" + shardPort;
          zk = new ZooKeeperClient(getZkConnectionString(), 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
          });
          String onlineShardsPath = ZookeeperPathConstants
              .getOnlineShardsPath(org.apache.blur.utils.BlurConstants.BLUR_CLUSTER);
          String path = onlineShardsPath + "/" + nodeName;
          zk.delete(path, -1);
          zk.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (zk != null) {
            try {
              zk.close();
            } catch (InterruptedException e) {
              LOG.error("Unknown error while trying to close ZooKeeper client.", e);
            }
          }
        }
      }

      @Override
      public void start() {
        try {
          thriftServer.start();
        } catch (TTransportException e) {
          LOG.error(e);
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void waitUntilOnline() {
        while (true) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            return;
          }
          int localPort = ThriftServer.getBindingPort(thriftServer.getServerTransport());
          if (localPort == 0) {
            continue;
          } else {
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              LOG.error("Unknown error", e);
            }
            return;
          }
        }
      }

    };
  }

  private ThriftServer getThriftServer(int serverIndex, TYPE type, BlurConfiguration configuration) throws Exception {
    switch (type) {
    case controller:
      return ThriftBlurControllerServer.createServer(serverIndex, configuration);
    case shard:
      return ThriftBlurShardServer.createServer(serverIndex, configuration);
    default:
      throw new RuntimeException("Type not supported [" + type + "]");
    }
  }

  public void startShards(int num, boolean randomPort, boolean externalProcesses) {
    BlurConfiguration configuration = getBlurConfiguration();
    startShards(configuration, num, randomPort, externalProcesses);
  }

  public void startShards(final BlurConfiguration configuration, int num, final boolean randomPort,
      final boolean externalProcesses) {
    final BlurConfiguration localConf = getBlurConfiguration(configuration);
    if (randomPort) {
      localConf.setInt(BLUR_SHARD_BIND_PORT, 0);
    }
    ExecutorService executorService = Executors.newFixedThreadPool(num);
    List<Future<MiniClusterServer>> futures = new ArrayList<Future<MiniClusterServer>>();
    for (int i = 0; i < num; i++) {
      final int index = i;
      futures.add(executorService.submit(new Callable<MiniClusterServer>() {
        @Override
        public MiniClusterServer call() throws Exception {
          return toMiniClusterServer(index, TYPE.shard, localConf, externalProcesses);
        }
      }));
    }
    for (int i = 0; i < num; i++) {
      try {
        MiniClusterServer server = futures.get(i).get();
        shards.add(server);
        startServer(server);
      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  public void killShardServer(int shardServer) throws IOException, InterruptedException, KeeperException {
    killShardServer(getBlurConfiguration(), shardServer);
  }

  public void killShardServer(final BlurConfiguration configuration, int shardServer) throws IOException,
      InterruptedException, KeeperException {
    MiniClusterServer miniClusterServer = shards.get(shardServer);
    miniClusterServer.kill();
  }

  private static void startServer(final MiniClusterServer server) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    }).start();
    server.waitUntilOnline();
  }

  public Configuration getConfiguration() {
    return _conf;
  }

  public String getZkConnectionString() {
    return zkMiniCluster.getZkConnectionString();
  }

  public void startZooKeeper(String path) {
    zkMiniCluster.startZooKeeper(path);
  }

  public void startZooKeeper(String path, boolean randomPort) {
    zkMiniCluster.startZooKeeper(path, randomPort);
  }

  public void startZooKeeper(boolean format, String path) {
    zkMiniCluster.startZooKeeper(format, path);
  }

  public void startZooKeeper(boolean format, String path, boolean randomPort) {
    zkMiniCluster.startZooKeeper(format, path, randomPort);
  }

  public void startZooKeeper(Properties properties, String path) {
    zkMiniCluster.startZooKeeper(properties, path);
  }

  public void startZooKeeper(Properties properties, String path, boolean randomPort) {
    zkMiniCluster.startZooKeeper(properties, path, randomPort);
  }

  public void startZooKeeper(final Properties properties, boolean format, String path, final boolean randomPort) {
    zkMiniCluster.startZooKeeper(properties, format, path, randomPort);
  }

  public FileSystem getFileSystem() throws IOException {
    return cluster.getFileSystem();
  }

  public URI getFileSystemUri() throws IOException {
    return getFileSystem().getUri();
  }

  public void startDfs(String path) {
    startDfs(true, path);
  }

  public void startDfs(boolean format, String path) {
    startDfs(new Configuration(), format, path);
  }

  public void startDfs(Configuration conf, String path) {
    startDfs(conf, true, path);
  }

  public void startDfs(final Configuration conf, final boolean format, final String path) {
    startDfs(conf, format, path, null);
  }

  public void startDfs(final Configuration conf, final boolean format, final String path, final String[] racks) {
    Thread thread = new Thread(group, new Runnable() {
      @SuppressWarnings("deprecation")
      @Override
      public void run() {
        _conf = conf;
        String perm;
        Path p = new Path(new File(path).getAbsolutePath());
        try {
          FileSystem fileSystem = p.getFileSystem(conf);
          if (!fileSystem.exists(p)) {
            if (!fileSystem.mkdirs(p)) {
              throw new RuntimeException("Could not create path [" + path + "]");
            }
          }
          FileStatus fileStatus = fileSystem.getFileStatus(p);
          FsPermission permission = fileStatus.getPermission();
          perm = permission.getUserAction().ordinal() + "" + permission.getGroupAction().ordinal() + ""
              + permission.getOtherAction().ordinal();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        LOG.info("dfs.datanode.data.dir.perm=" + perm);
        conf.set("dfs.datanode.data.dir.perm", perm);
        System.setProperty("test.build.data", path);
        try {
          if (racks == null) {
            cluster = new MiniDFSCluster(conf, 1, format, racks);
          } else {
            cluster = new MiniDFSCluster(conf, racks.length, format, racks);
          }
        } catch (Exception e) {
          LOG.error("error opening file system", e);
          throw new RuntimeException(e);
        }
      }
    });
    thread.start();
    try {
      thread.join();
      cluster.waitActive();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdownZooKeeper() {
    zkMiniCluster.shutdownZooKeeper();
  }

  public void shutdownDfs() {
    if (cluster != null) {
      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // / Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
        FileSystem.closeAll();
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }

      // This has got to be one of the worst hacks I have ever had to do.
      // This is needed to shutdown 2 thread pools that are not shutdown by
      // themselves.
      ThreadGroup threadGroup = group;
      Thread[] threads = new Thread[100];
      int enumerate = threadGroup.enumerate(threads);
      for (int i = 0; i < enumerate; i++) {
        Thread thread = threads[i];
        if (thread.getName().startsWith("pool")) {
          if (thread.isAlive()) {
            thread.interrupt();
            LOG.info("Stopping ThreadPoolExecutor [" + thread.getName() + "]");
            Object target = getField(Thread.class, thread, "target");
            if (target != null) {
              ThreadPoolExecutor e = (ThreadPoolExecutor) getField(ThreadPoolExecutor.class, target, "this$0");
              if (e != null) {
                e.shutdownNow();
              }
            }
            try {
              LOG.info("Waiting for thread pool to exit [" + thread.getName() + "]");
              thread.join();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
  }

  private static Object getField(Class<?> c, Object o, String fieldName) {
    try {
      Field field = c.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(o);
    } catch (NoSuchFieldException e) {
      try {
        Field field = o.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(o);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
