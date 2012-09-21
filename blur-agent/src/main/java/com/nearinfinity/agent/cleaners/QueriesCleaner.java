package com.nearinfinity.agent.cleaners;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import com.nearinfinity.agent.connections.interfaces.QueryDatabaseInterface;

public class QueriesCleaner implements Runnable {
  private static final Log log = LogFactory.getLog(QueriesCleaner.class);

  // Time in hours (removes TTL hour old queries)
  private final int timeToLive = -2;
  // Time in minutes
  private final int runningTimeToLive = -2;

  private final QueryDatabaseInterface database;

  public QueriesCleaner(final QueryDatabaseInterface database) {
    this.database = database;
  }

  @Override
  public void run() {
    try {
      Calendar now = Calendar.getInstance();
      TimeZone z = now.getTimeZone();
      now.add(Calendar.MILLISECOND, -(z.getOffset(new Date().getTime())));

      Calendar ttlHoursAgo = Calendar.getInstance();
      ttlHoursAgo.setTimeInMillis(now.getTimeInMillis());
      ttlHoursAgo.add(Calendar.HOUR_OF_DAY, timeToLive);

      Calendar ttlMinutesAgo = Calendar.getInstance();
      ttlMinutesAgo.setTimeInMillis(now.getTimeInMillis());
      ttlMinutesAgo.add(Calendar.MINUTE, runningTimeToLive);

      int deletedQueries = this.database.deleteOldQueries(ttlHoursAgo.getTime());
      int expiredQueries = this.database.expireOldQueries(ttlHoursAgo.getTime(), now.getTime());
      log.info("Removed " + deletedQueries + " queries and " + "Expired " + expiredQueries
          + " queries, in this pass!");
    } catch (DataAccessException e) {
      log.error("An error occured while deleting queries from the database!", e);
    } catch (Exception e) {
      log.error("An unkown error occured while cleaning up the queries!", e);
    }
  }
}
