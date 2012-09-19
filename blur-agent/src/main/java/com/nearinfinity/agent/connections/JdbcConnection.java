package com.nearinfinity.agent.connections;

import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcConnection {
  public static JdbcTemplate createDBConnection(Properties props) {
    String url = props.getProperty("store.url");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    dataSource.setUrl(url);
    dataSource.setUsername(props.getProperty("store.user"));
    dataSource.setPassword(props.getProperty("store.password"));
    dataSource.setMaxActive(80);
    dataSource.setMinIdle(2);
    dataSource.setMaxWait(10000);
    dataSource.setMaxIdle(-1);
    dataSource.setRemoveAbandoned(true);
    dataSource.setRemoveAbandonedTimeout(60);
    dataSource.setDefaultAutoCommit(true);
    
    return new JdbcTemplate(dataSource);
  }
}
