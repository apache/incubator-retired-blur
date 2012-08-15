package com.nearinfinity.blur;

import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BLOCKCACHE_SLAB_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.Connection;
import com.nearinfinity.blur.thrift.ThriftBlurControllerServer;
import com.nearinfinity.blur.thrift.ThriftBlurShardServer;
import com.nearinfinity.blur.thrift.ThriftServer;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;

public abstract class MiniCluster {

  private static Log LOG = LogFactory.getLog(MiniCluster.class);
  private static MiniDFSCluster cluster;
  private static Thread serverThread;
  private static String zkConnectionString = "localhost:21810";
  private static ZooKeeperServerMainEmbedded zooKeeperServerMain;
  private static List<ThriftServer> controllers = new ArrayList<ThriftServer>();
  private static List<ThriftServer> shards = new ArrayList<ThriftServer>();
  private static String controllerConnectionStr;

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException, BlurException, TException {
    startDfs("./tmp");
    startZooKeeper("./tmp");

    startControllers(1);
    startShards(1);

    try {
      createTable("test");
      Iface client = BlurClient.getClient(getControllerConnectionStr());
      for (int i = 0; i < 100; i++) {
        addRow("test", i, client);
      }
      
      Thread.sleep(10000);

      for (int i = 0; i < 100; i++) {
        searchRow("test", i, client);
      }

    } finally {
      stopShards();
      stopControllers();

      shutdownZooKeeper();
      shutdownDfs();
    }
  }

  private static void createTable(String test) throws BlurException, TException, IOException {
    final TableDescriptor descriptor = new TableDescriptor();
    descriptor.setName(test);
    descriptor.setShardCount(7);
    descriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    descriptor.setTableUri(getFileSystemUri() + "/blur/" + test);
    BlurClientManager.execute(getControllerConnectionStr(), new BlurCommand<Void>() {
      @Override
      public Void call(Client client) throws BlurException, TException {
        client.createTable(descriptor);
        return null;
      }
    });
  }

  public static String getControllerConnectionStr() {
    return controllerConnectionStr;
  }

  private static void addRow(String table, int i, Iface client) throws BlurException, TException {
    Row row = new Row();
    row.setId(Integer.toString(i));
    Record record = new Record();
    record.setRecordId(Integer.toString(i));
    record.setFamily("test");
    record.addToColumns(new Column("test", Integer.toString(i)));
    row.addToRecords(record);
    client.mutate(BlurUtil.toRowMutation(table, row));
  }

  private static void searchRow(String table, int i, Iface client) throws BlurException, TException {
    // @TODO
    BlurQuery blurQuery = BlurUtil.newSimpleQuery("test.test:" + i);
    System.out.println("Running [" + blurQuery + "]");
    BlurResults results = client.query(table, blurQuery);
    if (results.getTotalResults() != 1L) {
      throw new RuntimeException("we got a problem here.");
    }
  }

  public static void stopControllers() {
    for (ThriftServer s : controllers) {
      s.close();
    }
  }

  public static void stopShards() {
    for (ThriftServer s : shards) {
      s.close();
    }
  }

  public static void startControllers(int num) {
    BlurConfiguration configuration = getBlurConfiguration();
    startControllers(configuration, num);
  }

  private static BlurConfiguration getBlurConfiguration() {
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
    return configuration;
  }

  public static void startControllers(BlurConfiguration configuration, int num) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < num; i++) {
      try {
        ThriftServer server = ThriftBlurControllerServer.createServer(i, configuration);
        controllers.add(server);
        Connection connection = new Connection("localhost", 40010 + i);
        if (builder.length() != 0) {
          builder.append(',');
        }
        builder.append(connection.getConnectionStr());
        startServer(server, connection);
      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
    controllerConnectionStr = builder.toString();
  }

  public static void startShards(int num) {
    BlurConfiguration configuration = getBlurConfiguration();
    startShards(configuration, num);
  }

  public static void startShards(BlurConfiguration configuration, int num) {
    for (int i = 0; i < num; i++) {
      try {
        ThriftServer server = ThriftBlurShardServer.createServer(i, configuration);
        shards.add(server);
        Connection connection = new Connection("localhost", 40020 + i);
        startServer(server, connection);
      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  private static void startServer(final ThriftServer server, Connection connection) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          server.start();
        } catch (TTransportException e) {
          LOG.error(e);
          throw new RuntimeException(e);
        }
      }
    }).start();
    while (true) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        return;
      }
      try {
        Client client = BlurClientManager.newClient(connection);
        BlurClientManager.close(client);
        break;
      } catch (TException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        LOG.info("Can not connection to [" + connection + "]");
      }
    }
  }

  public static String getZkConnectionString() {
    return zkConnectionString;
  }

  public static void startZooKeeper(String path) {
    startZooKeeper(true, path);
  }

  public static void startZooKeeper(boolean format, String path) {
    Properties properties = new Properties();
    properties.setProperty("tickTime", "2000");
    properties.setProperty("initLimit", "10");
    properties.setProperty("syncLimit", "5");

    properties.setProperty("clientPort", "21810");

    startZooKeeper(properties, format, path);
  }

  public static void startZooKeeper(Properties properties, String path) {
    startZooKeeper(properties, true, path);
  }

  private static class ZooKeeperServerMainEmbedded extends ZooKeeperServerMain {
    @Override
    public void shutdown() {
      super.shutdown();
    }
  }

  public static void startZooKeeper(final Properties properties, boolean format, String path) {
    String realPath = path + "/zk_test";
    properties.setProperty("dataDir", realPath);
    final ServerConfig serverConfig = new ServerConfig();
    QuorumPeerConfig config = new QuorumPeerConfig();
    try {
      config.parseProperties(properties);
    } catch (IOException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    } catch (ConfigException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    serverConfig.readFrom(config);
    rm(new File(realPath));
    serverThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          zooKeeperServerMain = new ZooKeeperServerMainEmbedded();
          zooKeeperServerMain.runFromConfig(serverConfig);
        } catch (IOException e) {
          LOG.error(e);
        }
      }
    });
    serverThread.start();
    long s = System.nanoTime();
    while (s + 10000000000L > System.nanoTime()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
      try {
        ZooKeeper zk = new ZooKeeper(zkConnectionString, 30000, new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
        zk.close();
        break;
      } catch (IOException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  public static URI getFileSystemUri() throws IOException {
    return cluster.getFileSystem().getUri();
  }

  public static void startDfs(String path) {
    startDfs(true, path);
  }

  public static void startDfs(boolean format, String path) {
    startDfs(new Configuration(), format, path);
  }

  public static void startDfs(Configuration conf, String path) {
    startDfs(conf, true, path);
  }

  public static void startDfs(Configuration conf, boolean format, String path) {
    System.setProperty("test.build.data", path);
    try {
      cluster = new MiniDFSCluster(conf, 1, true, (String[]) null);
      cluster.waitActive();
    } catch (Exception e) {
      LOG.error("error opening file system", e);
      throw new RuntimeException(e);
    }
  }

  public static void shutdownZooKeeper() {
    zooKeeperServerMain.shutdown();
  }

  public static void shutdownDfs() {
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
      ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      Thread[] threads = new Thread[100];
      int enumerate = threadGroup.enumerate(threads);
      for (int i = 0; i < enumerate; i++) {
        Thread thread = threads[i];
        if (thread.getName().startsWith("pool")) {
          while (thread.isAlive()) {
            LOG.info("Stopping ThreadPoolExecutor [" + thread.getName() + "]");
            Object target = getField(thread, "target");
            if (target != null) {
              ThreadPoolExecutor e = (ThreadPoolExecutor) getField(target, "this$0");
              if (e != null) {
                e.shutdownNow();
              }
            }
          }
        }
      }

    }
  }

  private static Object getField(Object o, String fieldName) {
    try {
      Field field = o.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

}
