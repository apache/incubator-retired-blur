package com.nearinfinity.agent.connections.hdfs;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;

public class HdfsDatabaseConnection implements HdfsDatabaseInterface {
  private final JdbcTemplate jdbc;
  private final int timeToLive = -14;

  public HdfsDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public void setHdfsInfo(String name, String host, int port) {
    List<Map<String, Object>> existingHdfs = jdbc.queryForList("select id from hdfs where name=?",
        name);

    if (existingHdfs.isEmpty()) {
      jdbc.update("insert into hdfs (name, host, port) values (?, ?, ?)", name, host, port);
    } else {
      jdbc.update("update hdfs set host=?, port=? where id=?", host, port,
          existingHdfs.get(0).get("ID"));
    }
  }

  @Override
  public int getHdfsId(String name) throws NullReturnedException {
    int id = jdbc.queryForInt("select id from hdfs where name = ?", name);
    if (id == 0) {
      throw new NullReturnedException();
    }
    return id;
  }

  @Override
  public void insertHdfsStats(long capacity, long presentCapacity, long remaining, long used,
      long logical_used, double d, long underReplicatedBlocksCount, long corruptBlocksCount,
      long missingBlocksCount, long totalNodes, long liveNodes, long deadNodes, Date time,
      String host, int port, int hdfsId) {
    jdbc.update(
        "insert into hdfs_stats (config_capacity, present_capacity, dfs_remaining, dfs_used_real, dfs_used_logical, dfs_used_percent, under_replicated, corrupt_blocks, missing_blocks, total_nodes, live_nodes, dead_nodes, created_at, host, port, hdfs_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        capacity, presentCapacity, remaining, used, logical_used,
        (((1.0 * used) / presentCapacity) * 100), underReplicatedBlocksCount, corruptBlocksCount,
        missingBlocksCount, totalNodes, liveNodes, deadNodes, time, host, port, hdfsId);
  }

  @Override
  public int deleteOldStats() {
    Calendar now = getUTCCal(new Date().getTime());

    Calendar ttlDaysAgo = Calendar.getInstance();
    ttlDaysAgo.setTimeInMillis(now.getTimeInMillis());
    ttlDaysAgo.add(Calendar.DATE, timeToLive);
    
    return jdbc.update("delete from hdfs_stats where created_at < ?", ttlDaysAgo);
  }
  
  private static Calendar getUTCCal(long timeToStart) {
    Calendar cal = Calendar.getInstance();
    TimeZone z = cal.getTimeZone();
    cal.add(Calendar.MILLISECOND, -(z.getOffset(timeToStart)));
    return cal;
  }
}
