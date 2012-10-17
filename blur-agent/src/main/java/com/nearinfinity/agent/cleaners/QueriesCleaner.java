package com.nearinfinity.agent.cleaners;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;

import com.nearinfinity.agent.connections.blur.interfaces.QueryDatabaseInterface;

public class QueriesCleaner implements Runnable {
  private static final Log log = LogFactory.getLog(QueriesCleaner.class);

  private final QueryDatabaseInterface database;

  public QueriesCleaner(final QueryDatabaseInterface database) {
    this.database = database;
  }

  @Override
  public void run() {
    try {
      int deletedQueries = this.database.deleteOldQueries();
      int expiredQueries = this.database.expireOldQueries();
      log.info("Removed " + deletedQueries + " queries and " + "Expired " + expiredQueries
          + " queries, in this pass!");
    } catch (DataAccessException e) {
      log.error("An error occured while deleting queries from the database!", e);
    } catch (Exception e) {
      log.error("An unkown error occured while cleaning up the queries!", e);
    }
  }
}
