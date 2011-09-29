package com.nearinfinity.agent.collectors;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.springframework.jdbc.core.JdbcTemplate;

public class HDFSCollector {
	public static void startCollecting(final String uriString, final String name, final JdbcTemplate jdbc) {
		try {
			System.out.println("HDFS Collector");
			new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						String[] uriParts = uriString.split(":");
						String host = uriParts[0];
						String port = uriParts[1];
						
						int hdfsId = jdbc.queryForInt("select id from hdfs where name = ?", name);
						
						URI uri = new URI(uriString);
						FileSystem fileSystem = FileSystem.get(uri, new Configuration());
	
						System.out.println(fileSystem.getClass());
						if (fileSystem instanceof DistributedFileSystem) {
							DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
	
							FsStatus ds = dfs.getStatus();
							long capacity = ds.getCapacity();
							long used = ds.getUsed();
							long remaining = ds.getRemaining();
							long presentCapacity = used + remaining;
	
							System.out.println("Configured Capacity: " + capacity);
							System.out.println("Present Capacity: " + presentCapacity);
							System.out.println("DFS Remaining: " + remaining);
							System.out.println("DFS Used: " + used);
							System.out.println("DFS Used%: " + (((1.0 * used) / presentCapacity) * 100) + "%");
							System.out.println("Under replicated blocks: " + dfs.getUnderReplicatedBlocksCount());
							System.out.println("Blocks with corrupt replicas: " + dfs.getCorruptBlocksCount());
							System.out.println("Missing blocks: " + dfs.getMissingBlocksCount());
							System.out.println();
							System.out.println("-------------------------------------------------");
							
							long liveNodes = -1;
							long deadNodes = -1;
							long totalNodes = -1;
							
							try {
								DatanodeInfo[] live = dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
								DatanodeInfo[] dead = dfs.getClient().datanodeReport(DatanodeReportType.DEAD);
								System.out.println("Datanodes available: " + live.length + " ("	+ (live.length + dead.length) + " total, " + dead.length + " dead)\n");
								
								liveNodes = live.length;
								deadNodes = dead.length;
								totalNodes = liveNodes + deadNodes;
							} catch (Exception e) {
								System.out.println("Access denied for user. Skipping node information.");
							}
							
							Calendar cal = Calendar.getInstance();
							TimeZone z = cal.getTimeZone();
							cal.add(Calendar.MILLISECOND, -(z.getOffset(cal.getTimeInMillis())));
							
							jdbc.update("insert into hdfs_stats (config_capacity, present_capacity, dfs_remaining, dfs_used, dfs_used_percent, under_replicated, corrupt_blocks, missing_blocks, total_nodes, live_nodes, dead_nodes, created_at, host, port, hdfs_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
										capacity,
										presentCapacity,
										remaining,
										used,
										(((1.0 * used) / presentCapacity) * 100),
										dfs.getUnderReplicatedBlocksCount(),
										dfs.getCorruptBlocksCount(),
										dfs.getMissingBlocksCount(),
										totalNodes,
										liveNodes,
										deadNodes,
										cal.getTime(),
										host,
										port,
										hdfsId);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void initializeHdfs(String name, String uriString, JdbcTemplate jdbc) {
		List<Map<String, Object>> existingHdfs = jdbc.queryForList("select id from hdfs where name=?", name);
		
		if (existingHdfs.isEmpty()) {
			URI uri;
			try {
				uri = new URI(uriString);
				jdbc.update("insert into hdfs (name, host, port) values (?, ?, ?)", name, uri.getHost(), uri.getPort());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		} else {
			URI uri;
			try {
				uri = new URI(uriString);
				jdbc.update("update hdfs set host=?, port=? where id=?", uri.getHost(), uri.getPort(), existingHdfs.get(0).get("ID"));
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}
}
