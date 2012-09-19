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
import com.nearinfinity.agent.exceptions.HdfsThreadException;
import com.nearinfinity.blur.thrift.BlurClient;

public class HdfsThreadManager implements Runnable {
  private static final Log log = LogFactory.getLog(HDFSCollector.class);

  private final String hdfsName;
  private final String user;
  private final String host;
  private final int port;
  private final AgentDatabaseInterface databaseConnection;
  private final JdbcTemplate jdbc;
  private final boolean collectHdfs;

  public HdfsThreadManager(final String hdfsName, final String defaultUri, final String thriftUri,
      final String user, AgentDatabaseInterface databaseConnection, List<String> activeCollectors,
      final JdbcTemplate jdbc) throws HdfsThreadException {
    try {
      initializeHdfs(hdfsName, thriftUri, jdbc);
      URI uri = new URI(defaultUri);

      this.hdfsName = hdfsName;
      this.user = user;
      this.host = uri.getHost();
      this.port = uri.getPort();
      this.databaseConnection = databaseConnection;
      this.jdbc = jdbc;
      this.collectHdfs = activeCollectors.contains("hdfs");

    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
      throw new HdfsThreadException();
    } catch (Exception e) {
      log.error("An unkown error occured while creating the collector.", e);
      throw new HdfsThreadException();
    }
  }

  @Override
  public void run() {
    while (true) {
      if (this.collectHdfs) {
        new Thread(new HDFSCollector(BlurClient.getClient(resolvedConnection), zookeeperName,
            new HdfsDatabaseConnection(this.jdbc)), "Table Collector - " + this.zookeeperName)
            .start();
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  private void initializeHdfs(String name, String thriftUri, JdbcTemplate jdbc)
      throws URISyntaxException {
    List<Map<String, Object>> existingHdfs = jdbc.queryForList("select id from hdfs where name=?",
        name);

    URI uri = new URI(thriftUri);
    
    this.databaseConnection.setHdfsInfo(name, thriftUri, port);

    if (existingHdfs.isEmpty()) {
      jdbc.update("insert into hdfs (name, host, port) values (?, ?, ?)", name, uri.getHost(),
          uri.getPort());
    } else {
      jdbc.update("update hdfs set host=?, port=? where id=?", uri.getHost(), uri.getPort(),
          existingHdfs.get(0).get("ID"));
    }
  }
}
