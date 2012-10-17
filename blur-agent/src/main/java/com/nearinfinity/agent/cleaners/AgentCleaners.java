package com.nearinfinity.agent.cleaners;

import java.util.List;

import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;

public class AgentCleaners implements Runnable {

  private final boolean cleanQueries;
  private final boolean cleanHdfsStats;
  private final CleanerDatabaseInterface database;

  public AgentCleaners(final List<String> activeCollectors, CleanerDatabaseInterface database) {
    this.cleanQueries = activeCollectors.contains("queries");
    this.cleanHdfsStats = activeCollectors.contains("hdfs");
    this.database = database;
  }

  @Override
  public void run() {
    while (true) {
      if (this.cleanQueries) {
        new Thread(new QueriesCleaner(this.database),
            "Query Cleaner Started").start();
      }

      if (this.cleanHdfsStats) {
        new Thread(new HdfsStatsCleaner(this.database),
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
