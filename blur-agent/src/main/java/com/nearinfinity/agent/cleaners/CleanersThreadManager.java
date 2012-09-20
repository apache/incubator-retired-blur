package com.nearinfinity.agent.cleaners;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.connections.HdfsDatabaseConnection;
import com.nearinfinity.agent.connections.QueryDatabaseConnection;

public class CleanersThreadManager implements Runnable {

  private final boolean cleanQueries;
  private final boolean cleanHdfsStats;
  private final JdbcTemplate jdbc;

  public CleanersThreadManager(final List<String> activeCollectors, JdbcTemplate jdbc) {
    this.cleanQueries = activeCollectors.contains("queries");
    this.cleanHdfsStats = activeCollectors.contains("hdfs");
    this.jdbc = jdbc;
  }

  @Override
  public void run() {
    while (true) {
      if (this.cleanQueries) {
        new Thread(new QueriesCleaner(new QueryDatabaseConnection(this.jdbc)),
            "Query Cleaner Started").start();
      }

      if (this.cleanHdfsStats) {
        new Thread(new HdfsStatsCleaner(new HdfsDatabaseConnection(this.jdbc)),
            "Hdfs Stats Cleaner Started").start();
      }

      try {
        Thread.sleep(Agent.CLEAN_UP_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

}
