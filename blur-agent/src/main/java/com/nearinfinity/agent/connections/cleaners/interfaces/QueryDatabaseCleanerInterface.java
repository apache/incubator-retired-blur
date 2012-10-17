package com.nearinfinity.agent.connections.cleaners.interfaces;

public interface QueryDatabaseCleanerInterface {
  int deleteOldQueries();

  int expireOldQueries();

}
