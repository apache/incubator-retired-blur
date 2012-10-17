package com.nearinfinity.agent.connections.cleaners;

import java.util.Calendar;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;
import com.nearinfinity.agent.types.TimeHelper;

public class CleanerDatabaseConnection implements CleanerDatabaseInterface {
  private final JdbcTemplate jdbc;
  
  // Query Expiration Times
  private final int expireThreshold = -2;
  private final int deleteThreshold = -2;
  
  // Hdfs Expiration Times
  private final int timeToLive = -14;

  public CleanerDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }
  
  @Override
  public int deleteOldQueries() {
    Calendar now = TimeHelper.now();

    Calendar twoHoursAgo = Calendar.getInstance();
    twoHoursAgo.setTimeInMillis(now.getTimeInMillis());
    twoHoursAgo.add(Calendar.HOUR_OF_DAY, deleteThreshold);

    return this.jdbc.update("delete from blur_queries where created_at < ?", twoHoursAgo.getTime());
  }

  @Override
  public int expireOldQueries() {
    Calendar now = TimeHelper.now();

    Calendar twoMinutesAgo = Calendar.getInstance();
    twoMinutesAgo.setTimeInMillis(now.getTimeInMillis());
    twoMinutesAgo.add(Calendar.MINUTE, expireThreshold);

    return this.jdbc.update("update blur_queries set state=1, updated_at=? where updated_at < ? and state = 0", now.getTime(),
        twoMinutesAgo);
  }
  
  @Override
  public int deleteOldStats() {
    Calendar now = TimeHelper.now();

    Calendar ttlDaysAgo = Calendar.getInstance();
    ttlDaysAgo.setTimeInMillis(now.getTimeInMillis());
    ttlDaysAgo.add(Calendar.DATE, timeToLive);
    
    return jdbc.update("delete from hdfs_stats where created_at < ?", ttlDaysAgo);
  }

}
