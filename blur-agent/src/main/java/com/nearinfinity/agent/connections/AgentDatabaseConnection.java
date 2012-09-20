package com.nearinfinity.agent.connections;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.AgentDatabaseInterface;

public class AgentDatabaseConnection implements AgentDatabaseInterface {
  private final JdbcTemplate jdbc;

  public AgentDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public String getConnectionString(String zookeeperName) {
    String queryString = "select distinct c.node_name from controllers c, zookeepers z where z.name = ? and c.zookeeper_id = z.id and c.status = 1";
    List<String> controller_uris = jdbc.queryForList(queryString, new String[] { zookeeperName },
        String.class);
    return StringUtils.join(controller_uris, ',');
  }
}
