package com.nearinfinity.agent.collectors;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONValue;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.TableMap;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;

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
							List<Map<String, Object>> existingRow = jdbc.queryForList("select id, complete, cpu_time, real_time, interrupted, running from blur_queries where blur_table_id=? and uuid=?", new Object[]{TableMap.get().get(table), blurQueryStatus.getUuid()});
							
							if (existingRow.isEmpty()) {
								Calendar cal = Calendar.getInstance();
								TimeZone z = cal.getTimeZone();
								cal.add(Calendar.MILLISECOND, -(z.getOffset(cal.getTimeInMillis())));
								
								jdbc.update("insert into blur_queries (query_string, cpu_time, real_time, complete, interrupted, running, uuid, created_at, updated_at, blur_table_id, super_query_on, facets, start, fetch_num, pre_filters, post_filters, selector_column_families, selector_columns, userid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
											new Object[]{blurQueryStatus.getQuery().getQueryStr(), 
												blurQueryStatus.getCpuTime(),
												blurQueryStatus.getRealTime(),
												blurQueryStatus.getComplete(),
												blurQueryStatus.isInterrupted(),
												blurQueryStatus.isRunning(),
												blurQueryStatus.getUuid(),
//												new Date(blurQueryStatus.getQuery().getStartTime()),
												cal.getTime(),
												cal.getTime(),
												TableMap.get().get(table),
												blurQueryStatus.getQuery().isSuperQueryOn(),
												StringUtils.join(blurQueryStatus.getQuery().getFacets(), ", "),
												blurQueryStatus.getQuery().getStart(),
												blurQueryStatus.getQuery().getFetch(),
												blurQueryStatus.getQuery().getPreSuperFilter(),
												blurQueryStatus.getQuery().getPostSuperFilter(),
												blurQueryStatus.getQuery().getSelector() == null ? null : JSONValue.toJSONString(blurQueryStatus.getQuery().getSelector().getColumnFamiliesToFetch()),
												blurQueryStatus.getQuery().getSelector() == null ? null : JSONValue.toJSONString(blurQueryStatus.getQuery().getSelector().getColumnsToFetch()),
												blurQueryStatus.getQuery().getUserId()
											});
							} else if (queryHasChanged(blurQueryStatus, existingRow.get(0))){
								Calendar cal = Calendar.getInstance();
								TimeZone z = cal.getTimeZone();
								cal.add(Calendar.MILLISECOND, -(z.getOffset(cal.getTimeInMillis())));
								
								jdbc.update("update blur_queries set cpu_time=?, real_time=?, complete=?, interrupted=?, running=?, updated_at=? where id=?", 
											new Object[] {blurQueryStatus.getCpuTime(),
												blurQueryStatus.getRealTime(),
												blurQueryStatus.getComplete(),
												blurQueryStatus.isInterrupted(),
												blurQueryStatus.isRunning(),
												cal.getTime(),
												existingRow.get(0).get("ID")
											});
							}
						}
						
					}
					return null;
				}

				private boolean queryHasChanged(BlurQueryStatus blurQueryStatus, Map<String, Object> map) {
					return !(blurQueryStatus.getRealTime() == (Integer)map.get("REAL_TIME") && 
							blurQueryStatus.getCpuTime() == (Integer)map.get("CPU_TIME") &&
							blurQueryStatus.isInterrupted() == (Boolean)map.get("INTERRUPTED") &&
							blurQueryStatus.isRunning() == (Boolean)map.get("RUNNING") &&
							blurQueryStatus.getComplete() == (Integer)map.get("COMPLETE"));
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
