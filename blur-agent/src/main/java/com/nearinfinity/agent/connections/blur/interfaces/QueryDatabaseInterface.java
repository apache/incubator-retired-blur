package com.nearinfinity.agent.connections.blur.interfaces;

import java.util.Date;
import java.util.Map;

import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public interface QueryDatabaseInterface {
  Map<String, Object> getQuery(int tableId, long UUID);

  void createQuery(BlurQueryStatus status, SimpleQuery query, String times, Date startTime, int tableId);

  void updateQuery(BlurQueryStatus status, String times, int queryId);
}
