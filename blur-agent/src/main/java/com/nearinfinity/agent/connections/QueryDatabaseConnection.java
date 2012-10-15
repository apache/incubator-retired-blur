package com.nearinfinity.agent.connections;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.QueryDatabaseInterface;

public class QueryDatabaseConnection implements QueryDatabaseInterface {

  private final JdbcTemplate jdbc;
  private final int expireThreshold = -2;
  private final int deleteThreshold = -2;

  public QueryDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public int deleteOldQueries() {
    Calendar now = getUTCCal(new Date().getTime());

    Calendar twoHoursAgo = Calendar.getInstance();
    twoHoursAgo.setTimeInMillis(now.getTimeInMillis());
    twoHoursAgo.add(Calendar.HOUR_OF_DAY, deleteThreshold);

    return jdbc.update("delete from blur_queries where created_at < ?", twoHoursAgo.getTime());
  }

  @Override
  public int expireOldQueries() {
    Calendar now = getUTCCal(new Date().getTime());

    Calendar twoMinutesAgo = Calendar.getInstance();
    twoMinutesAgo.setTimeInMillis(now.getTimeInMillis());
    twoMinutesAgo.add(Calendar.MINUTE, expireThreshold);

    return jdbc.update(
        "update blur_queries set state=1, updated_at=? where updated_at < ? and state = 0",
        now.getTime(), twoMinutesAgo);
  }

  private static Calendar getUTCCal(long timeToStart) {
    Calendar cal = Calendar.getInstance();
    TimeZone z = cal.getTimeZone();
    cal.add(Calendar.MILLISECOND, -(z.getOffset(timeToStart)));
    return cal;
  }
}
