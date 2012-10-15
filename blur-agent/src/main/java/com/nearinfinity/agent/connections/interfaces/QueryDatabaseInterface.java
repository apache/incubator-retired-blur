package com.nearinfinity.agent.connections.interfaces;

public interface QueryDatabaseInterface {
  int deleteOldQueries();

  int expireOldQueries();
}
