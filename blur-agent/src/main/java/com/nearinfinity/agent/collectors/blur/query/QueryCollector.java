package com.nearinfinity.agent.collectors.blur.query;

import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.nearinfinity.agent.connections.interfaces.QueryDatabaseInterface;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;

public class QueryCollector implements Runnable {
  private static final Log log = LogFactory.getLog(QueryCollector.class);

  private final Iface blurConnection;
  private final String tableName;
  private final QueryDatabaseInterface database;

  public QueryCollector(Iface connection, String tableName, QueryDatabaseInterface database) {
    this.blurConnection = connection;
    this.tableName = tableName;
    this.database = database;
  }

  @Override
  public void run() {
    List<Long> currentQueries;
    try {
      currentQueries = blurConnection.queryStatusIdList(tableName);
    } catch (Exception e) {
      log.error("Unable to get the list of current queries [" + tableName + "].", e);
      return;
    }
    
    for (Long queryUUID : currentQueries) {
      BlurQueryStatus status;
      try {
        status = blurConnection.queryStatusById(tableName, queryUUID);
      } catch (Exception e) {
        log.error("Unable to get the shard schema for table [" + tableName + "].", e);
        continue;
      }
      
    }
  }

  private static boolean queryHasChanged(BlurQueryStatus blurQueryStatus, String timesJSON,
      Map<String, Object> map) {
    return blurQueryStatus.getState().getValue() == 0
        || !(timesJSON.equals(map.get("TIMES"))
            && blurQueryStatus.getCompleteShards() == (Integer) map.get("COMPLETE_SHARDS") && blurQueryStatus
            .getState().getValue() == (Integer) map.get("STATE"));
  }
}
