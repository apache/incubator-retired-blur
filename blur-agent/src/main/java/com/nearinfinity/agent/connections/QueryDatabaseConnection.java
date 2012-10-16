package com.nearinfinity.agent.connections;

import java.util.Calendar;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONValue;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.QueryDatabaseInterface;
import com.nearinfinity.agent.types.TimeHelper;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public class QueryDatabaseConnection implements QueryDatabaseInterface {

  private final JdbcTemplate jdbc;
  private final int expireThreshold = -2;
  private final int deleteThreshold = -2;

  public QueryDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @Override
  public int deleteOldQueries() {
    Calendar now = TimeHelper.now();

    Calendar twoHoursAgo = Calendar.getInstance();
    twoHoursAgo.setTimeInMillis(now.getTimeInMillis());
    twoHoursAgo.add(Calendar.HOUR_OF_DAY, deleteThreshold);

    return this.jdbc.update("delete from blur_queries where created_at < ?", twoHoursAgo.getTime());
  }

  @Override
  public int expireOldQueries() {
    Calendar now = TimeHelper.now();

    Calendar twoMinutesAgo = Calendar.getInstance();
    twoMinutesAgo.setTimeInMillis(now.getTimeInMillis());
    twoMinutesAgo.add(Calendar.MINUTE, expireThreshold);

    return this.jdbc.update(
        "update blur_queries set state=1, updated_at=? where updated_at < ? and state = 0",
        now.getTime(), twoMinutesAgo);
  }

  public Map<String, Object> getQuery(long UUID) {
    return this.jdbc
        .queryForMap(
            "select id, complete_shards, times, state from blur_queries where blur_table_id=? and uuid=?",
            UUID);
  }

  public void createQuery(BlurQueryStatus status, SimpleQuery query, String times, long startTime, int tableId) {
    this.jdbc.update(
            "insert into blur_queries (query_string, times, complete_shards, total_shards, state, uuid, created_at, updated_at, blur_table_id, super_query_on, facets, start, fetch_num, pre_filters, post_filters, selector_column_families, selector_columns, userid, record_only) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            query.getQueryStr(),
            times,
            status.getCompleteShards(),
            status.getTotalShards(),
            status.getState().getValue(),
            status.getUuid(),
            startTime,
            TimeHelper.now().getTime(),
            tableId,
            query.isSuperQueryOn(),
            StringUtils.join(status.getQuery().getFacets(), ", "),
            status.getQuery().getStart(),
            status.getQuery().getFetch(),
            query.getPreSuperFilter(),
            query.getPostSuperFilter(),
            status.getQuery().getSelector() == null ? null : JSONValue.toJSONString(status
                .getQuery().getSelector().getColumnFamiliesToFetch()),
            status.getQuery().getSelector() == null ? null : JSONValue.toJSONString(status
                .getQuery().getSelector().getColumnsToFetch()), status.getQuery().getUserContext(),
            status.getQuery().getSelector() == null ? null : status.getQuery().getSelector()
                .isRecordOnly());
  }
  
  public void updateQuery(BlurQueryStatus status, String times, int queryId){
    jdbc.update("update blur_queries set times=?, complete_shards=?, total_shards=?, state=?, updated_at=? where id=?", 
      times,
      status.getCompleteShards(),
      status.getTotalShards(),
      status.getState().getValue(),
      TimeHelper.now().getTime(),
      queryId);
  }
}
