package com.nearinfinity.agent.connections;

import java.util.Date;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.QueryDatabaseInterface;

public class QueryDatabaseConnection implements QueryDatabaseInterface {
  
  private final JdbcTemplate jdbc;

  public QueryDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public int deleteOldQueries(Date threshold) {
    return jdbc.update("delete from blur_queries where created_at < ?", threshold);
  }
}
