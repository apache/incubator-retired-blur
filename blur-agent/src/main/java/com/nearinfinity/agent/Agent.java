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
import java.util.List;
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
		
		
		if (args.length == 0) {
			System.out.println("Config file location must be the first argument.");
			System.exit(1);
		} 
		
		String configParam = args[0];
		
		File configFile = new File(configParam);
		
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
		
		new Agent(configProps);
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
		
		JdbcTemplate jdbc = new JdbcTemplate(dataSource);
		
		//Initialize ZooKeeper watchers
		List<String> zooKeeperInstances = new ArrayList<String>(Arrays.asList(props.getProperty("zk.instances").split("\\|")));
		System.out.println(zooKeeperInstances);
		for (String zkInstance : zooKeeperInstances) {
			String zkUrl = props.getProperty("zk."+zkInstance+".url");
			System.out.println(zkUrl);
			new ZookeeperInstance(zkInstance, zkUrl, jdbc);
		}
		
		List<String> activeCollectors = new ArrayList<String>(Arrays.asList(props.getProperty("active.collectors").split("\\|")));
		
		//Start polling
		while(true) {
			//Pull HDFS information
			if (activeCollectors.contains("hdfs")) {
				try {
					HDFSCollector.startCollecting(props.getProperty("hdfs.url"), jdbc);
				} catch (Exception e) {
					System.out.println("Unable to collect HDFS stats, will try again next pass: " + e.getMessage());
				}
//				HDFSCollector.startCollecting("hdfs://192.168.64.130:8020/");
			}
			//Pull Query information
			if (activeCollectors.contains("queries")) {
				try {
					QueryCollector.startCollecting(props.getProperty("blur.url"), jdbc);
				} catch (Exception e) {
					System.out.println("Unable to collect Query status, will try again next pass: " + e.getMessage());
				}
			}
			//Pull Table information
			if (activeCollectors.contains("tables")) {
				try {
					TableCollector.startCollecting(props.getProperty("blur.url"), jdbc);
				} catch (Exception e) {
					System.out.println("Unable to collect Table information, will try again next pass: " + e.getMessage());
				}
			}
			
			//Sleep
			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {
				break;
			}
		}
		
		System.out.println("Exiting polling agent");
	}
}