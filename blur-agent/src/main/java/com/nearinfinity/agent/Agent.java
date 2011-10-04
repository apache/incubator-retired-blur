package com.nearinfinity.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.nearinfinity.agent.collectors.HDFSCollector;
import com.nearinfinity.agent.collectors.QueryCollector;
import com.nearinfinity.agent.collectors.TableCollector;
import com.nearinfinity.agent.zookeeper.ZookeeperInstance;

public class Agent {

	public static void main(String[] args) throws ParseException {
		writePidFile();		
		Properties configProps = loadConfigParams(args);
		new Agent(configProps);
	}

	private static Properties loadConfigParams(String[] args) {
		if (args.length == 0) {
			System.out.println("Config file location must be the first argument.");
			System.exit(1);
		} 
		
		File configFile = new File(args[0]);
		
		if (!configFile.exists() || !configFile.isFile()) {
			System.out.println("Unable to find config file at " + configFile.getAbsolutePath());
			System.exit(1);
		}
		
		Properties configProps = new Properties();
		try {
			configProps.load(new FileInputStream(configFile));
		} catch (Exception e) {
			System.out.println("Config File is not a valid properties file: " + e.getMessage());
			System.exit(1);
		}
		return configProps;
	}

	private static void writePidFile() {
		try {
			File pidFile = new File("agent.pid");
			PrintWriter pidOut = new PrintWriter(pidFile);
			System.out.println("Wrote pid file to: " + pidFile.getAbsolutePath());
			String nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName();  
		    int p = nameOfRunningVM.indexOf('@');  
		    String pid = nameOfRunningVM.substring(0, p);
		    pidOut.write(pid);
		    pidOut.close();
		} catch (FileNotFoundException e) {
			System.out.println("Unable to find pid file. " + e.getMessage());
			System.exit(1);
		}
	}
	
	public Agent(Properties props) {
		//Setup database connection
		String url = props.getProperty("store.url");
		SimpleDriverDataSource dataSource = null;
		try {
			dataSource = new SimpleDriverDataSource(DriverManager.getDriver(url), url, props.getProperty("store.user"), props.getProperty("store.password"));
		} catch (SQLException e) {
			System.out.println("Unable to connect to the collector store: " + e.getMessage());
			System.exit(1);
		}
		
		final JdbcTemplate jdbc = new JdbcTemplate(dataSource);
		
		//Initialize ZooKeeper watchers
		List<String> zooKeeperInstances = new ArrayList<String>(Arrays.asList(props.getProperty("zk.instances").split("\\|")));
		for (String zkInstance : zooKeeperInstances) {
			String zkUrl = props.getProperty("zk."+zkInstance+".url");
			new Thread(new ZookeeperInstance(zkInstance, zkUrl, jdbc, props)).start();
		}
		
		List<String> activeCollectors = new ArrayList<String>(Arrays.asList(props.getProperty("active.collectors").split("\\|")));
		
		Map<String, Map<String, String>> hdfsInstances = loadHdfsInstances(props);
		Map<String, String> blurInstances = loadBlurInstances(props);
		
		for (Map.Entry<String, Map<String, String>> hdfsEntry : hdfsInstances.entrySet()) {
			HDFSCollector.initializeHdfs(hdfsEntry.getKey(), hdfsEntry.getValue().get("thrift"), jdbc);
		}
		
		//Pull HDFS information
		if (activeCollectors.contains("hdfs")) {
			for (Map<String, String> instance : hdfsInstances.values()) {
				try {
					new Thread(new Runnable(){
						@Override
						public void run() {
							while(true) {
								//HDFSCollector.startCollecting(instance.get("default"), jdbc);
								try {
									Thread.sleep(1500);
								} catch (InterruptedException e) {
									break;
								}
							}
						}
					}).start();
				} catch (Exception e) {
					System.out.println("Unable to collect HDFS stats, will try again next pass: " + e.getMessage());
				}
			}
		}
		//Pull Query information
		if (activeCollectors.contains("queries")) {
			for (final String uri : blurInstances.values()) {
				try {
					new Thread(new Runnable(){
						@Override
						public void run() {
							while(true) {
								QueryCollector.startCollecting(uri, jdbc);
								try {
									Thread.sleep(1500);
								} catch (InterruptedException e) {
									break;
								}
							}
						}
					}).start();
				} catch (Exception e) {
					System.out.println("Unable to collect Query status, will try again next pass: " + e.getMessage());
				}
			}
		}
		//Pull Table information
		if (activeCollectors.contains("tables")) {
			for (final String uri : blurInstances.values()) {
				try {
					new Thread(new Runnable(){
						@Override
						public void run() {
							while (true) {
								TableCollector.startCollecting(uri, jdbc);
								try {
									Thread.sleep(1500);
								} catch (InterruptedException e) {
									break;
								}
							}
						}
					}).start();
				} catch (Exception e) {
					System.out.println("Unable to collect Table information, will try again next pass: " + e.getMessage());
				}
			}
		}

		
		System.out.println("Exiting polling agent");
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
				instanceInfo.put("thrift", props.getProperty("hdfs.thrift." + hdfs + ".url"));
				instanceInfo.put("default", props.getProperty("hdfs." + hdfs + ".url"));
				instances.put(hdfs, instanceInfo);
			}
		}
		
		return instances;
	}
}