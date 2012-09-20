package com.nearinfinity.agent.connections.interfaces;

import java.util.Date;

public interface QueryDatabaseInterface {
  int deleteOldQueries(Date threshold);
}
