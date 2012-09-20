package com.nearinfinity.agent.cleaners;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.springframework.jdbc.core.JdbcTemplate;

public class HdfsStatsCleaner implements Runnable {
  @Override
  public void run() {
    // TODO Auto-generated method stub
    
  }
  
  public static void cleanStats(JdbcTemplate jdbc) {
    Calendar now = Calendar.getInstance();
    TimeZone z = now.getTimeZone();
    now.add(Calendar.MILLISECOND, -(z.getOffset(new Date().getTime())));

    Calendar oneWeekAgo = Calendar.getInstance();
    oneWeekAgo.setTimeInMillis(now.getTimeInMillis());
    oneWeekAgo.add(Calendar.DATE, -7);

    jdbc.update("delete from hdfs_stats where created_at < ?", oneWeekAgo.getTime());
  }
}
