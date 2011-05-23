package com.nearinfinity.agent.collectors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDFSCollector {
	public static void startCollecting() {
//		public static void startCollecting(String uriString) {
		try {
//			URI uri = new URI(uriString);
//	        final FileSystem fileSystem = FileSystem.get(uri,new Configuration());
	        
	        new Thread(new Runnable(){
	        	
				@Override
				public void run() {
					boolean loop = true;
					Pattern pattern = Pattern.compile(".*: (\\d*.?\\d*) *.*");
					Pattern dnPattern = Pattern.compile(".*\\((\\d*) total, (\\d*).*");
					
					while (loop) {
						try {
							Process p = Runtime.getRuntime().exec("hadoop dfsadmin -report");
							p.waitFor();
							BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream()));
							
							String configuredCapacity = reader.readLine();
							
							if (configuredCapacity == null) {
								System.out.println("Unable to generate statistics because admin report didn't return anything");
							} else {
								configuredCapacity = doMatches(configuredCapacity, pattern).group(1);
								
								String presentCapacity = doMatches(reader.readLine(), pattern).group(1);
								String dfsRemaining = doMatches(reader.readLine(), pattern).group(1);
								String dfsUsed = doMatches(reader.readLine(), pattern).group(1);
								String dfsUsedPercent = doMatches(reader.readLine(), pattern).group(1);
								String underReplicatedBlocks = doMatches(reader.readLine(), pattern).group(1);
								String blocksWithCorruptReplicas = doMatches(reader.readLine(), pattern).group(1);
								String missingBlocks = doMatches(reader.readLine(), pattern).group(1);
								
								reader.readLine();
								reader.readLine();
								
								String datanodeLine = reader.readLine();
								Matcher dnMatcher = doMatches(datanodeLine, dnPattern);
								String totalDatanodes = dnMatcher.group(1);
								String deadDatanodes = dnMatcher.group(2);
								
								System.out.println("Configured Capacity: " + configuredCapacity);
								System.out.println("Present Capacity: " + presentCapacity);
								System.out.println("DFS Remaining: " + dfsRemaining);
								System.out.println("DFS Used: " + dfsUsed);
								System.out.println("DFS Used %: " + dfsUsedPercent);
								System.out.println("Under replicated: " + underReplicatedBlocks);
								System.out.println("Corrupt Blocks: " + blocksWithCorruptReplicas);
								System.out.println("Missing Blocks: " + missingBlocks);
								System.out.println("Total nodes: " + totalDatanodes);
								System.out.println("Dead nodes: " + deadDatanodes);
							}						
						
						
//							System.out.println(fileSystem.getUsed());
//							
//							List<Statistics> allStatistics = FileSystem.getAllStatistics();
//							for (Statistics stats : allStatistics) {
//								System.out.println(stats.getScheme());
//								System.out.println(stats.getBytesRead());
//								System.out.println(stats.getBytesWritten());
//							}
							
//							FileSystem.printStatistics();
							
//							FileStatus[] listStatus = fileSystem.listStatus(new Path("file:///Users"));
//							for (FileStatus fileStatus : listStatus) {
//								System.out.println(fileStatus.getPath().toString());
//							}
						} catch (Exception e) {
							System.out.println("Unable to retrieve HDFS statistics. " + e.getMessage());
							e.printStackTrace();
						}
						
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							loop = false;
						}
					}
				}
				
				private Matcher doMatches(String input, Pattern pattern) {
					Matcher m = pattern.matcher(input);
					m.matches();
					return m;
				}
	        }).start();
		} catch (Exception e) {
			System.out.println("Unable to connect to HDFS. " + e.getMessage());
		}
	}
}
