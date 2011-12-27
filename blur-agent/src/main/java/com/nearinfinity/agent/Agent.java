package com.nearinfinity.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.collectors.HDFSCollector;
import com.nearinfinity.agent.collectors.QueryCollector;
import com.nearinfinity.agent.collectors.TableCollector;
import com.nearinfinity.agent.zookeeper.ZookeeperInstance;
import com.nearinfinity.license.CryptoServices;
import com.nearinfinity.license.CryptoServicesException;
import com.nearinfinity.license.IssuingKey;

public class Agent {
	private static final Log log = LogFactory.getLog(Agent.class);
	
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
	
	public Agent(Properties props) {
		
		//Setup database connection
		JdbcTemplate jdbc = setupDBConnection(props);
		
		//Verify valid License
		verifyLicense(props, jdbc);
		
		//Initialize ZooKeeper watchers
		initializeWatchers(props, jdbc);
		
		List<String> activeCollectors = props.containsKey("active.collectors") ? new ArrayList<String>(Arrays.asList(props.getProperty("active.collectors").split("\\|"))) : new ArrayList<String>();
		
		//Setup HDFS collectors
		setupHdfs(props, jdbc, activeCollectors);
		
		//Setup Blur collectors
		setupBlur(props, jdbc, activeCollectors);
		
		while (true) {
			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {
				break;
			}
		}
		
		log.info("Exiting agent");
	}
	
	private void verifyLicense(Properties props, JdbcTemplate jdbc) {
		String licenseFilePath = props.getProperty("license.file");
		
		if (StringUtils.isBlank(licenseFilePath)) {
			log.fatal("Missing license.file configuration property.  Exiting.");
			System.exit(1);
		}
		
		File licenseFile = new File(licenseFilePath);
		List<String> licenseFileLines = null;
		try {
			licenseFileLines = IOUtils.readLines(new FileInputStream(licenseFile));
		} catch (FileNotFoundException e) {
			log.fatal("Unable to find license file (" + licenseFile.getAbsolutePath() + ").  Exiting.");
			System.exit(1);
		} catch (IOException e) {
			log.fatal("There was a problem reading the license file: " + e.getMessage() + ". Exiting");
			System.exit(1);
		}
		
		String licenseData = StringUtils.join(licenseFileLines.subList(1, 7),"\n");
		String signature = StringUtils.join(licenseFileLines.subList(7, licenseFileLines.size()-1), "\n");
		
		CryptoServices cryptoServices = CryptoServices.getCryptoServices();
		IssuingKey issuingKey = new IssuingKey();
		issuingKey.setDescription("Main");
		issuingKey.setPrivateKey(CryptoServices.getCryptoServices().decodeBase64("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"));
		issuingKey.setPublicKey(CryptoServices.getCryptoServices().decodeBase64("MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAr70Zcg1WlzllR6xuxJEBuaETSbgAHY6IuZ9lqLWPEdj1I8cQdk28NONmsqMj0jHBjDmqby1YYIsj3+kxib6Ow2rxL/r2EXsdzZxXTdVCDzJMX1JqVSKLqXmbYB7gy9xLqxxDYCVZwTmhAQmhU1KN1PA6IzvyNFcPfG11qyzrfwqAJ+8rA4556KtEe9EbWBPh3PEEqcuq7lQAFXrrr32cvluSVgqgTF2eLYaRyyPxgvBTkFSg4OkdzT2gP+ggiL9SBXA9y6yLZZR2ehMxS5GhS+96n95wLF5CIbg4onefQzZ65/VGwaSeWbPM/8RBUcK/jRC99rNcF6phywJCiKMgJ+u9cj1ThuIBm5LhSs2FreCuDNtUkna4ernk0Ndy1vSpTu0+h5IiMbz2cbfDJq++w8Os7J/Pt3XfgON2cHcJyEEbkzsJ71Z8+lebMydzX/al95FyXDrobuUuUfOVNrXWKqFB+BRIHPTVJo2tP9FGjcxhmj5vHQkGEAh/0n9b93cCBN6e//ft9TP/yELT+lSFJIeJOHj8gUvSFT5JfilTjZLHvLweMR9ZVlfseq1NC8EoYWgpa6PQcUh2PzuTBgoCyRCG4/mIqw45ICuLeS/MwGERJqWpxWPKRpSbklWDrFObOrfaPb+bvKzsPvbf0Q1x+Azqr9d4OSm7DunNAtV85aUCAwEAAQ=="));
		
		try {
			if (!cryptoServices.verify(licenseData.toString().getBytes(), cryptoServices.decodeBase64(signature), cryptoServices.getPublicKey(issuingKey.getPublicKey()))) {
				log.fatal("Invalid license.  Exiting");
				System.exit(1);
			}
		} catch (CryptoServicesException e) {
			log.fatal("There was a problem decrypting license.  Exiting.");
			System.exit(1);
		}
		
		//Add information to db about license
		jdbc.update("delete from licenses");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			jdbc.update("insert into licenses (org, issued_date, expires_date) values (?,?,?)", licenseFileLines.get(1), sdf.parse(licenseFileLines.get(4)), sdf.parse(licenseFileLines.get(5)));
		} catch (Exception e) {
			log.fatal("Bad date formats in license.  Exiting.");
			System.exit(1);
		}
	}

	private void setupBlur(Properties props, final JdbcTemplate jdbc, List<String> activeCollectors) {
		Map<String, String> blurInstances = loadBlurInstances(props);
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
					log.warn("Unable to collect Query status, will try again next pass: " + e.getMessage());
				}
			}
		}
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
					log.warn("Unable to collect Table information, will try again next pass: " + e.getMessage());
				}
			}
		}

	}

	private void setupHdfs(Properties props, final JdbcTemplate jdbc, List<String> activeCollectors) {
		Map<String, Map<String, String>> hdfsInstances = loadHdfsInstances(props);
		for (Map.Entry<String, Map<String, String>> hdfsEntry : hdfsInstances.entrySet()) {
			HDFSCollector.initializeHdfs(hdfsEntry.getKey(), hdfsEntry.getValue().get("thrift"), jdbc);
		}
		
		if (activeCollectors.contains("hdfs")) {
			for (final Map<String, String> instance : hdfsInstances.values()) {
				try {
					new Thread(new Runnable(){
						@Override
						public void run() {
							while(true) {
								HDFSCollector.startCollecting(instance.get("default"), instance.get("name"), jdbc);
								try {
									Thread.sleep(1500);
								} catch (InterruptedException e) {
									break;
								}
							}
						}
					}).start();
				} catch (Exception e) {
					log.warn("Unable to collect HDFS stats, will try again next pass: " + e.getMessage());
				}
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
				new Thread(new ZookeeperInstance(zkInstance, zkUrl, jdbc, props)).start();
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
				instanceInfo.put("thrift", props.getProperty("hdfs.thrift." + hdfs + ".url"));
				instanceInfo.put("default", props.getProperty("hdfs." + hdfs + ".url"));
				instanceInfo.put("name", hdfs);
				instances.put(hdfs, instanceInfo);
			}
		}
		
		return instances;
	}
}