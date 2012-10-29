package com.nearinfinity.agent.collectors.blur.query;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.nearinfinity.agent.connections.blur.interfaces.QueryDatabaseInterface;
import com.nearinfinity.agent.types.TimeHelper;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public class QueryCollector implements Runnable {
  private static final Log log = LogFactory.getLog(QueryCollector.class);

  private final Iface blurConnection;
  private final String tableName;
  private final int tableId;
  private final QueryDatabaseInterface database;

  public QueryCollector(Iface connection, String tableName, int tableId,
      QueryDatabaseInterface database) {
    this.blurConnection = connection;
    this.tableName = tableName;
    this.tableId = tableId;
    this.database = database;
  }

  @Override
  public void run() {
    Set<Long> currentQueries = new HashSet<Long>();
    try {
      currentQueries.addAll(blurConnection.queryStatusIdList(tableName));
      currentQueries.addAll(this.database.getRunningQueries());
    } catch (Exception e) {
      log.error("Unable to get the list of current queries [" + tableName + "].");
      return;
    }

    for (Long queryUUID : currentQueries) {
      BlurQueryStatus status;
      try {
        status = blurConnection.queryStatusById(tableName, queryUUID);
      } catch (Exception e) {
        log.error("Unable to get query status for query [" + queryUUID + "].");
        continue;
      }

      Map<String, Object> oldQuery = this.database.getQuery(this.tableId, queryUUID);

      String times;
      try {
        times = new ObjectMapper().writeValueAsString(status.getCpuTimes());
      } catch (Exception e) {
        log.error("Unable to parse cpu times.", e);
        times = null;
      }

      if (oldQuery == null) {
        SimpleQuery query = status.getQuery().getSimpleQuery();
        long startTimeLong = status.getQuery().getStartTime();

        // Set the query creation time to now or given start time
        Date startTime = (startTimeLong > 0) ?
            TimeHelper.getAdjustedTime(startTimeLong).getTime() :
            TimeHelper.now().getTime();

        this.database.createQuery(status, query, times, startTime, this.tableId);
      } else if (queryHasChanged(status, times, oldQuery)) {
        this.database.updateQuery(status, times, (Integer) oldQuery.get("ID"));
      }
    }
  }

  private static boolean queryHasChanged(BlurQueryStatus blurQueryStatus, String timesJSON,
      Map<String, Object> oldQueryInfo) {
    return blurQueryStatus.getState().getValue() == 0
        || !(timesJSON.equals(oldQueryInfo.get("TIMES"))
            && blurQueryStatus.getCompleteShards() == (Integer) oldQueryInfo.get("COMPLETE_SHARDS") && blurQueryStatus
            .getState().getValue() == (Integer) oldQueryInfo.get("STATE"));
  }
}
