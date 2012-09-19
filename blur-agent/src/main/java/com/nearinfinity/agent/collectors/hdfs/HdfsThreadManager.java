package com.nearinfinity.agent.collectors.hdfs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.collectors.blur.query.QueryCollector;
import com.nearinfinity.agent.collectors.blur.table.TableCollector;
import com.nearinfinity.agent.connections.blur.TableDatabaseConnection;
import com.nearinfinity.agent.connections.interfaces.AgentDatabaseInterface;
import com.nearinfinity.blur.thrift.BlurClient;

public class HdfsThreadManager implements Runnable {
  private static final Log log = LogFactory.getLog(HDFSCollector.class);
  
  private final String hdfsName;
  private final String user;
  private final String host;
  private final int port;
  private final JdbcTemplate jdbc;

  private String connection;

  public HdfsThreadManager(final String hdfsName, final String uriString, final String user,
      final JdbcTemplate jdbc) {
    try {
      URI uri = new URI(uriString);
      this.host = uri.getHost();
      this.port = uri.getPort();
      this.hdfsName = hdfsName;
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
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
        new Thread(new QueryCleaner(BlurClient.getClient(resolvedConnection), zookeeperName,
            new QueryDatabaseConnection(this.jdbc)), "Query Cleaner - " + this.zookeeperName)
            .start();
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
  }
  
  private static void initializeHdfs(String name, String thriftUri, JdbcTemplate jdbc) {
    List<Map<String, Object>> existingHdfs = jdbc.queryForList("select id from hdfs where name=?",
        name);

    if (existingHdfs.isEmpty()) {
      URI uri;
      try {
        uri = new URI(uriString);
        jdbc.update("insert into hdfs (name, host, port) values (?, ?, ?)", name, uri.getHost(),
            uri.getPort());
      } catch (URISyntaxException e) {
        log.debug(e);
      }
    } else {
      URI uri;
      try {
        uri = new URI(uriString);
        jdbc.update("update hdfs set host=?, port=? where id=?", uri.getHost(), uri.getPort(),
            existingHdfs.get(0).get("ID"));
      } catch (URISyntaxException e) {
        log.debug(e);
      }
    }
  }
}
