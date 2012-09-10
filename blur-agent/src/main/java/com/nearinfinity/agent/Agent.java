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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.collectors.blur.QueryCollector;
import com.nearinfinity.agent.collectors.blur.TableCollector;
import com.nearinfinity.agent.collectors.hdfs.HDFSCollector;
import com.nearinfinity.agent.collectors.zookeeper.ZookeeperInstance;
import com.nearinfinity.license.AgentLicense;

public class Agent {
	private static final Log log = LogFactory.getLog(Agent.class);
  private static final long COLLECTOR_SLEEP_TIME = TimeUnit.SECONDS.toMillis(15);
  private static final long CLEAN_UP_SLEEP_TIME = TimeUnit.HOURS.toMillis(1);
	
  private Agent(Properties props) {
    
    //Setup database connection
    JdbcTemplate jdbc = setupDBConnection(props);
    
    //Verify valid License
    AgentLicense.verifyLicense(props, jdbc);
    
    //Initialize ZooKeeper watchers
    initializeWatchers(props, jdbc);
    
    List<String> activeCollectors = props.containsKey("active.collectors") ? new ArrayList<String>(Arrays.asList(props.getProperty("active.collectors").split("\\|"))) : new ArrayList<String>();
    
    //Setup HDFS collectors
    setupHdfs(props, jdbc, activeCollectors);
    
    //Setup Blur collectors
    setupBlur(props, jdbc, activeCollectors);
    
    while (true) {
      try {
        Thread.sleep(COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
    
    log.info("Exiting agent");
  }
  
	public static void main(String[] args) {
		writePidFile();		
		Properties configProps = loadConfigParams(args);
		setupLogger(configProps);
		new Agent(configProps);
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
	
	private void setupBlur(Properties props, final JdbcTemplate jdbc, List<String> activeCollectors) {
		Map<String, String> blurInstances = loadBlurInstances(props);
		if (activeCollectors.contains("tables")) {
			for (Map.Entry<String, String> blurEntry : blurInstances.entrySet()) {
				final String zookeeper = blurEntry.getKey();
				final String connection = blurEntry.getValue();
				
				new Thread(new Runnable(){
					@Override
					public void run() {
						while (true) {
							if (StringUtils.isBlank(connection)) {
								List<String> controller_uris = jdbc.queryForList("select distinct c.node_name from controllers c, zookeepers z where z.name = ? and c.zookeeper_id = z.id and c.status = 1", new String[]{zookeeper}, String.class);
								TableCollector.startCollecting(StringUtils.join(controller_uris, ','), zookeeper, jdbc);
							} else {
								TableCollector.startCollecting(connection, zookeeper, jdbc);
							}
							
							try {
								Thread.sleep(COLLECTOR_SLEEP_TIME);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}, "Table Collector - " + zookeeper).start();
			}
		}
		if (activeCollectors.contains("queries")) {
			for (Map.Entry<String, String> blurEntry : blurInstances.entrySet()) {
				final String zookeeper = blurEntry.getKey();
				final String connection = blurEntry.getValue();
				
				new Thread(new Runnable(){
					@Override
					public void run() {
						try {
							Thread.sleep(TimeUnit.SECONDS.toMillis(5));
						} catch (InterruptedException e) {
							return;
						}
						while(true) {
							if (StringUtils.isBlank(connection)) {
								List<String> controller_uris = jdbc.queryForList("select distinct c.node_name from controllers c, zookeepers z where z.name = ? and c.zookeeper_id = z.id and status = 1", new String[]{zookeeper}, String.class);
								QueryCollector.startCollecting(StringUtils.join(controller_uris, ','), zookeeper, jdbc);
							} else {
								QueryCollector.startCollecting(connection, zookeeper, jdbc);
							}

							try {
								Thread.sleep(COLLECTOR_SLEEP_TIME);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}, "Query Collector - " + zookeeper).start();
				new Thread(new Runnable(){
					@Override
					public void run() {
						while(true) {
							QueryCollector.cleanQueries(jdbc);
							try {
								Thread.sleep(CLEAN_UP_SLEEP_TIME);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}, "Query Cleaner - " + zookeeper).start();
			}
		}

	}

	private void setupHdfs(Properties props, final JdbcTemplate jdbc, List<String> activeCollectors) {
		Map<String, Map<String, String>> hdfsInstances = loadHdfsInstances(props);
		for (Map.Entry<String, Map<String, String>> hdfsEntry : hdfsInstances.entrySet()) {
			HDFSCollector.initializeHdfs(hdfsEntry.getKey(), hdfsEntry.getValue().get("thrift"), jdbc);
		}
		
		if (activeCollectors.contains("hdfs")) {
			for (Map<String, String> instance : hdfsInstances.values()) {
				final String uri = instance.get("default");
				final String name = instance.get("name");
				final String user = props.getProperty("hdfs." + name + ".login.user");
				new Thread(new Runnable(){
					@Override
					public void run() {
						while(true) {
							HDFSCollector.startCollecting(uri, name, user, jdbc);
							try {
								Thread.sleep(COLLECTOR_SLEEP_TIME);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}, "HDFS Collector - " + name).start();
				new Thread(new Runnable(){
					@Override
					public void run() {
						while(true) {
							HDFSCollector.cleanStats(jdbc);
							try {
								Thread.sleep(CLEAN_UP_SLEEP_TIME);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}, "HDFS Cleaner - " + name).start();
			}
		}
	}

	private JdbcTemplate setupDBConnection(Properties props) {
		String url = props.getProperty("store.url");
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");
		dataSource.setUrl(url);
		dataSource.setUsername(props.getProperty("store.user"));
		dataSource.setPassword(props.getProperty("store.password"));
		dataSource.setMaxActive(80);
		dataSource.setMinIdle(2);
		dataSource.setMaxWait(10000);
		dataSource.setMaxIdle(-1);
		dataSource.setRemoveAbandoned(true);
		dataSource.setRemoveAbandonedTimeout(60);
		dataSource.setDefaultAutoCommit(true);
		
		return new JdbcTemplate(dataSource);
	}
	
	private void initializeWatchers(Properties props, JdbcTemplate jdbc) {
		if (props.containsKey("zk.instances")) {
			List<String> zooKeeperInstances = new ArrayList<String>(Arrays.asList(props.getProperty("zk.instances").split("\\|")));
			for (String zkInstance : zooKeeperInstances) {
				String zkUrl = props.getProperty("zk."+zkInstance+".url");
				new Thread(new ZookeeperInstance(zkInstance, zkUrl, jdbc, props), "Zookeeper-" + zkInstance).start();
			}
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
				instanceInfo.put("thrift", props.getProperty("hdfs." + hdfs + ".thrift.url"));
				instanceInfo.put("default", props.getProperty("hdfs." + hdfs + ".url"));
				instanceInfo.put("name", hdfs);
				instances.put(hdfs, instanceInfo);
			}
		}
		
		return instances;
	}
}
