package com.nearinfinity.agent.cleaners;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import com.nearinfinity.agent.connections.interfaces.HdfsDatabaseInterface;

public class HdfsStatsCleaner implements Runnable {
  private static final Log log = LogFactory.getLog(QueriesCleaner.class);

  private final int timeToLive = -14;
  private final HdfsDatabaseInterface database;

  public HdfsStatsCleaner(HdfsDatabaseInterface database) {
    this.database = database;
  }

  @Override
  public void run() {
    try {
      Calendar now = Calendar.getInstance();
      TimeZone z = now.getTimeZone();
      now.add(Calendar.MILLISECOND, -(z.getOffset(new Date().getTime())));

      Calendar ttlDaysAgo = Calendar.getInstance();
      ttlDaysAgo.setTimeInMillis(now.getTimeInMillis());
      ttlDaysAgo.add(Calendar.DATE, timeToLive);

      this.database.deleteOldStats(ttlDaysAgo.getTime());
    } catch (DataAccessException e) {
      log.error("An error occured while deleting hdfs stats from the database!", e);
    } catch (Exception e) {
      log.error("An unkown error occured while cleaning up the hdfs stats!", e);
    }
  }
}
