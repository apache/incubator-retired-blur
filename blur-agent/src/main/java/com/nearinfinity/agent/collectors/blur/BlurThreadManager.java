package com.nearinfinity.agent.collectors.blur;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.collectors.blur.query.QueryCollector;
import com.nearinfinity.agent.collectors.blur.table.TableCollector;
import com.nearinfinity.agent.connections.TableDatabaseConnection;
import com.nearinfinity.agent.connections.interfaces.AgentDatabaseInterface;
import com.nearinfinity.blur.thrift.BlurClient;

public class BlurThreadManager implements Runnable {
  private final String zookeeperName;
  private final AgentDatabaseInterface databaseConnection;
  private final JdbcTemplate jdbc;
  private final boolean collectTables;
  private final boolean collectQueries;

  private String connection;

  public BlurThreadManager(final String zookeeperName, final String connection,
      final List<String> activeCollectors, final AgentDatabaseInterface databaseConnection,
      final JdbcTemplate jdbc) {
    this.zookeeperName = zookeeperName;
    this.connection = connection;
    this.databaseConnection = databaseConnection;
    this.collectTables = activeCollectors.contains("tables");
    this.collectQueries = activeCollectors.contains("queries");
    this.jdbc = jdbc;
  }

  @Override
  public void run() {
    while (true) {
      // If the connection string is blank then we need to build it from the
      // online controllers from the database
      String resolvedConnection = this.connection;
      if (StringUtils.isBlank(this.connection)) {
        resolvedConnection = this.databaseConnection.getConnectionString(this.zookeeperName);
      }

      if (this.collectTables) {
        new Thread(new TableCollector(BlurClient.getClient(resolvedConnection), zookeeperName,
            new TableDatabaseConnection(this.jdbc)), "Table Collector - " + this.zookeeperName)
            .start();
      }

      if (this.collectQueries) {
        // The queries depend on the tables collected above so we will wait
        // a short amount of time for the collectors above to complete
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
          return;
        }

        new Thread(new QueryCollector(BlurClient.getClient(resolvedConnection), zookeeperName,
            new QueryDatabaseConnection(this.jdbc)), "Query Collector - " + this.zookeeperName)
            .start();
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
  }
}
