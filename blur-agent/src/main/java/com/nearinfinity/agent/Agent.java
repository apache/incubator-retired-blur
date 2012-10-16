package com.nearinfinity.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.cleaners.AgentCleaners;
import com.nearinfinity.agent.collectors.blur.BlurCollector;
import com.nearinfinity.agent.collectors.hdfs.HdfsCollector;
import com.nearinfinity.agent.collectors.zookeeper.ZookeeperCollector;
import com.nearinfinity.agent.connections.BlurDatabaseConnection;
import com.nearinfinity.agent.connections.HdfsDatabaseConnection;
import com.nearinfinity.agent.connections.JdbcConnection;
import com.nearinfinity.agent.connections.ZookeeperDatabaseConnection;
import com.nearinfinity.agent.exceptions.HdfsThreadException;
import com.nearinfinity.license.AgentLicense;

public class Agent {
  public static final long COLLECTOR_SLEEP_TIME = TimeUnit.SECONDS.toMillis(15);
  public static final long CLEAN_UP_SLEEP_TIME = TimeUnit.HOURS.toMillis(1);

  private static final Log log = LogFactory.getLog(Agent.class);

  private Agent(Properties props) {

    // Setup database connection
    JdbcTemplate jdbc = JdbcConnection.createDBConnection(props);

    // Verify valid License
    AgentLicense.verifyLicense(props, jdbc);

    List<String> activeCollectors = props.containsKey("active.collectors") ? new ArrayList<String>(Arrays.asList(props.getProperty(
        "active.collectors").split("\\|"))) : new ArrayList<String>();

    // Setup the collectors
    setupHdfs(props, jdbc, activeCollectors);
    setupBlur(props, jdbc, activeCollectors);
    setupZookeeper(props, jdbc);
    setupCleaners(jdbc, activeCollectors);
  }

  public static void main(String[] args) {
    writePidFile();
    Properties configProps = loadConfigParams(args);
    setupLogger(configProps);
    new Agent(configProps);

    // Sleep the main thread while the background threads
    // are working
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      log.info("Exiting agent");
    }
  }

  private void setupCleaners(JdbcTemplate jdbc, List<String> activeCollectors) {
    new Thread(new AgentCleaners(activeCollectors, jdbc)).start();
  }

  private void setupBlur(Properties props, JdbcTemplate jdbc, List<String> activeCollectors) {
    Map<String, String> blurInstances = loadBlurInstances(props);
    for (Map.Entry<String, String> blurEntry : blurInstances.entrySet()) {
      final String zookeeperName = blurEntry.getKey();
      final String connection = blurEntry.getValue();
      new Thread(new BlurCollector(zookeeperName, connection, activeCollectors, new BlurDatabaseConnection(jdbc), jdbc)).start();
    }
  }

  private void setupHdfs(Properties props, final JdbcTemplate jdbc, List<String> activeCollectors) {
    Map<String, Map<String, String>> hdfsInstances = loadHdfsInstances(props);
    for (Map<String, String> instance : hdfsInstances.values()) {
      final String name = instance.get("name");
      final String thriftUri = instance.get("uri.thrift");
      final String defaultUri = instance.get("uri.default");
      final String user = props.getProperty("hdfs." + name + ".login.user");
      try {
        new Thread(new HdfsCollector(name, defaultUri, thriftUri, user, activeCollectors, new HdfsDatabaseConnection(jdbc))).start();
      } catch (HdfsThreadException e) {
        log.error("The collector for hdfs [" + name + "] will not execute.");
        continue;
      }
    }
  }

  private void setupZookeeper(Properties props, JdbcTemplate jdbc) {
    if (props.containsKey("zk.instances")) {
      List<String> zooKeeperInstances = new ArrayList<String>(Arrays.asList(props.getProperty("zk.instances").split("\\|")));
      for (String zkInstance : zooKeeperInstances) {
        String zkUrl = props.getProperty("zk." + zkInstance + ".url");
        new Thread(new ZookeeperCollector(zkInstance, zkUrl, new ZookeeperDatabaseConnection(jdbc)), "Zookeeper-" + zkInstance).start();
      }
    }
  }

  private static void setupLogger(Properties props) {
    String log4jPropsFile = props.getProperty("log4j.properties", "../conf/log4j.properties");

    if (new File(log4jPropsFile).exists()) {
      PropertyConfigurator.configure(log4jPropsFile);
    } else {
      log.warn("Unable to find log4j properties file.  Using default logging");
    }
  }

  private static Properties loadConfigParams(String[] args) {
    String configFileName;
    if (args.length == 0) {
      configFileName = "../conf/blur-agent.config";
    } else {
      configFileName = args[0];
    }
    File configFile = new File(configFileName);

    if (!configFile.exists() || !configFile.isFile()) {
      log.fatal("Unable to find config file at " + configFile.getAbsolutePath());
      System.exit(1);
    }

    Properties configProps = new Properties();
    try {
      configProps.load(new FileInputStream(configFile));
    } catch (Exception e) {
      log.fatal("Config File is not a valid properties file: " + e.getMessage());
      System.exit(1);
    }
    return configProps;
  }

  private static void writePidFile() {
    try {
      File pidFile = new File("../agent.pid");
      PrintWriter pidOut = new PrintWriter(pidFile);
      log.info("Wrote pid file to: " + pidFile.getAbsolutePath());
      String nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName();
      int p = nameOfRunningVM.indexOf('@');
      String pid = nameOfRunningVM.substring(0, p);
      pidOut.write(pid);
      pidOut.write("\n");
      pidOut.close();
    } catch (FileNotFoundException e) {
      log.fatal("Unable to find pid file. " + e.getMessage());
      System.exit(1);
    }
  }

  private Map<String, String> loadBlurInstances(Properties props) {
    Map<String, String> instances = new HashMap<String, String>();

    if (props.containsKey("blur.instances")) {
      String[] blurNames = props.getProperty("blur.instances").split("\\|");

      for (String blur : blurNames) {
        instances.put(blur, props.getProperty("blur." + blur + ".url"));
      }
    }

    return instances;
  }

  private Map<String, Map<String, String>> loadHdfsInstances(Properties props) {
    Map<String, Map<String, String>> instances = new HashMap<String, Map<String, String>>();

    if (props.containsKey("hdfs.instances")) {
      String[] hdfsNames = props.getProperty("hdfs.instances").split("\\|");

      for (String hdfs : hdfsNames) {
        Map<String, String> instanceInfo = new HashMap<String, String>();
        instanceInfo.put("url.thrift", props.getProperty("hdfs." + hdfs + ".thrift.url"));
        instanceInfo.put("url.default", props.getProperty("hdfs." + hdfs + ".url"));
        instanceInfo.put("name", hdfs);
        instances.put(hdfs, instanceInfo);
      }
    }

    return instances;
  }
}
