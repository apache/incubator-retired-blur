package com.nearinfinity.agent.collectors.blur;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.collectors.blur.query.QueryCollector;
import com.nearinfinity.agent.collectors.blur.table.TableCollector;
import com.nearinfinity.agent.connections.blur.interfaces.BlurDatabaseInterface;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurCollector implements Runnable {
  private static final Log log = LogFactory.getLog(BlurCollector.class);

  private final String zookeeperName;
  private final BlurDatabaseInterface database;
  private final boolean collectTables;
  private final boolean collectQueries;

  private String connection;

  public BlurCollector(final String zookeeperName, final String connection,
      final List<String> activeCollectors, final BlurDatabaseInterface database,
      final JdbcTemplate jdbc) {
    this.zookeeperName = zookeeperName;
    this.connection = connection;
    this.database = database;
    this.collectTables = activeCollectors.contains("tables");
    this.collectQueries = activeCollectors.contains("queries");
  }

  @Override
  public void run() {
    while (true) {
      // If the connection string is blank then we need to build it from the
      // online controllers from the database
      String resolvedConnection = getResolvedConnection();

      Iface blurConnection = BlurClient.getClient(resolvedConnection);

      /* Retrieve the zookeeper id */
      int zookeeperId = getZookeeperId();

      /* Retrieve the clusters and their info */
      for (Map<String, Object> cluster : this.database.getClusters(zookeeperId)) {
        String clusterName = (String) cluster.get("NAME");
        Integer clusterId = (Integer) cluster.get("ID");

        List<String> tables;
        try {
          tables = blurConnection.tableListByCluster(clusterName);
        } catch (Exception e) {
          log.error("An error occured while trying to retrieve the table list for cluster["
              + clusterName + "], skipping cluster", e);
          continue;
        }

        for (final String tableName : tables) {
          int tableId = this.database.getTableId(clusterId, tableName);

          if (this.collectTables) {
            new Thread(new TableCollector(BlurClient.getClient(resolvedConnection), tableName,
                tableId, this.database), "Table Collector - " + tableName).start();
          }

          if (this.collectQueries) {
            new Thread(new QueryCollector(BlurClient.getClient(resolvedConnection), tableName,
                tableId, this.database), "Query Collector - "
                + tableName).start();
          }
        }
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  private String getResolvedConnection() {
    if (StringUtils.isBlank(this.connection)) {
      return this.database.getConnectionString(this.zookeeperName);
      // ToDo: Write the connection string to the Database on the Zookeeper
      // object for the use of RoR
    } else {
      return this.connection;
    }
  }

  private int getZookeeperId() {
    try {
      return Integer.parseInt(this.database.getZookeeperId(this.zookeeperName));
    } catch (NumberFormatException e) {
      log.error("The returned zookeeperId is not a valid number", e);
      return -1;
    } catch (ZookeeperNameMissingException e) {
      log.error(e.getMessage(), e);
      return -1;
    } catch (ZookeeperNameCollisionException e) {
      log.error(e.getMessage(), e);
      return -1;
    }
  }
}
