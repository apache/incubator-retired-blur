package com.nearinfinity.agent.collectors;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * 
 * +------------------+---------------+------+-----+---------+----------------+
 * | Field            | Type          | Null | Key | Default | Extra          |
 * +------------------+---------------+------+-----+---------+----------------+
 * | id               | int(11)       | NO   | PRI | NULL    | auto_increment |
 * | hdfs_name        | varchar(255)  | YES  |     | NULL    |                |
 * | config_capacity  | int(11)       | YES  |     | NULL    |                |
 * | present_capacity | int(11)       | YES  |     | NULL    |                |
 * | dfs_remaining    | int(11)       | YES  |     | NULL    |                |
 * | dfs_used         | int(11)       | YES  |     | NULL    |                |
 * | dfs_used_percent | decimal(10,0) | YES  |     | NULL    |                |
 * | under_replicated | int(11)       | YES  |     | NULL    |                |
 * | corrupt_blocks   | int(11)       | YES  |     | NULL    |                |
 * | missing_blocks   | int(11)       | YES  |     | NULL    |                |
 * | total_nodes      | int(11)       | YES  |     | NULL    |                |
 * | dead_nodes       | int(11)       | YES  |     | NULL    |                |
 * | created_at       | datetime      | YES  |     | NULL    |                |
 * | updated_at       | datetime      | YES  |     | NULL    |                |
 * +------------------+---------------+------+-----+---------+----------------+
 */
public class HDFSCollector {
  public static void startCollecting(String uriString, JdbcTemplate jdbc) {
    try {
			URI uri = new URI(uriString);
      FileSystem fileSystem = FileSystem.get(uri,new Configuration());

      if (fileSystem instanceof DistributedFileSystem) {
        DistributedFileSystem dfs = (DistributedFileSystem)fileSystem;
        
        DiskStatus ds = dfs.getDiskStatus();
        long capacity = ds.getCapacity();
        long used = ds.getDfsUsed();
        long remaining = ds.getRemaining();
        long presentCapacity = used + remaining;

        System.out.println("Configured Capacity: " + capacity);
        System.out.println("Present Capacity: " + presentCapacity);
        System.out.println("DFS Remaining: " + remaining);
        System.out.println("DFS Used: " + used);
        System.out.println("DFS Used%: " + (((1.0 * used) / presentCapacity) * 100) + "%");
        System.out.println("Under replicated blocks: " + dfs.getUnderReplicatedBlocksCount());
        System.out.println("Blocks with corrupt replicas: " + dfs.getCorruptBlocksCount());
        System.out.println("Missing blocks: " +  dfs.getMissingBlocksCount());
        System.out.println();
        System.out.println("-------------------------------------------------");
        
        DatanodeInfo[] live = dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
        DatanodeInfo[] dead = dfs.getClient().datanodeReport(DatanodeReportType.DEAD);
        System.out.println("Datanodes available: " + live.length + " (" + (live.length + dead.length) + " total, " + dead.length + " dead)\n");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
