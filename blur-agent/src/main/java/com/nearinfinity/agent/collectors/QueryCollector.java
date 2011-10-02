package com.nearinfinity.agent.collectors;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONValue;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.TableMap;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;

public class QueryCollector {
	public static void startCollecting(String connection, final JdbcTemplate jdbc) {
		try {
			BlurClientManager.execute(connection, new BlurCommand<Void>() {
				@Override
				public Void call(Client client) throws Exception {
					jdbc.update("delete from blur_queries where created_at+0 < NOW() - (2*60*60);");
					
					List<String> tables = client.tableList();
					
					for (String table : tables) {
						List<BlurQueryStatus> currentQueries = client.currentQueries(table);
						
						for (BlurQueryStatus blurQueryStatus : currentQueries) {
							//Check if query exists
							List<Map<String, Object>> existingRow = jdbc.queryForList("select id, complete_shards, times, state from blur_queries where blur_table_id=? and uuid=?", new Object[]{TableMap.get().get(table), blurQueryStatus.getUuid()});
							
							ObjectMapper timesMapper = new ObjectMapper();
							String times = timesMapper.writeValueAsString(blurQueryStatus.getCpuTimes());
							
							if (existingRow.isEmpty()) {
								Calendar cal = Calendar.getInstance();
								TimeZone z = cal.getTimeZone();
								cal.add(Calendar.MILLISECOND, -(z.getOffset(cal.getTimeInMillis())));
								
								SimpleQuery query = blurQueryStatus.getQuery().getSimpleQuery();
								
								System.out.println(blurQueryStatus.getQuery().getSelector());
								
								jdbc.update("insert into blur_queries (query_string, times, complete_shards, total_shards, state, uuid, created_at, updated_at, blur_table_id, super_query_on, facets, start, fetch_num, pre_filters, post_filters, selector_column_families, selector_columns, userid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
											new Object[]{query.getQueryStr(), 
												times,
												blurQueryStatus.getCompleteShards(),
												blurQueryStatus.getTotalShards(),
												blurQueryStatus.getState().getValue(),
												blurQueryStatus.getUuid(),
//												new Date(blurQueryStatus.getQuery().getStartTime()),
												cal.getTime(),
												cal.getTime(),
												TableMap.get().get(table),
												query.isSuperQueryOn(),
												StringUtils.join(blurQueryStatus.getQuery().getFacets(), ", "),
												blurQueryStatus.getQuery().getStart(),
												blurQueryStatus.getQuery().getFetch(),
												query.getPreSuperFilter(),
												query.getPostSuperFilter(),
												blurQueryStatus.getQuery().getSelector() == null ? null : JSONValue.toJSONString(blurQueryStatus.getQuery().getSelector().getColumnFamiliesToFetch()),
												blurQueryStatus.getQuery().getSelector() == null ? null : JSONValue.toJSONString(blurQueryStatus.getQuery().getSelector().getColumnsToFetch()),
												blurQueryStatus.getQuery().getUserContext()
											});
							} else if (queryHasChanged(blurQueryStatus, times, existingRow.get(0))){
								Calendar cal = Calendar.getInstance();
								TimeZone z = cal.getTimeZone();
								cal.add(Calendar.MILLISECOND, -(z.getOffset(cal.getTimeInMillis())));
								
								jdbc.update("update blur_queries set times=?, complete_shards=?, total_shards=?, state=?, updated_at=? where id=?", 
											new Object[] {times,
												blurQueryStatus.getCompleteShards(),
												blurQueryStatus.getTotalShards(),
												blurQueryStatus.getState().getValue(),
												cal.getTime(),
												existingRow.get(0).get("ID")
											});
							}
						}
						
					}
					return null;
				}

				private boolean queryHasChanged(BlurQueryStatus blurQueryStatus, String timesJSON, Map<String, Object> map) {
					return !(timesJSON.equals(map.get("TIMES")) && 
							blurQueryStatus.getCompleteShards() == (Integer)map.get("COMPLETE_SHARDS") &&
							blurQueryStatus.getState().getValue() == (Integer)map.get("STATE"));
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
