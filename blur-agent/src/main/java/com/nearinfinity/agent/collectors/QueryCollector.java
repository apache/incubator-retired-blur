package com.nearinfinity.agent.collectors;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	private static final Log log = LogFactory.getLog(QueryCollector.class);
	
	public static void startCollecting(String connection, final String zookeeperName, final JdbcTemplate jdbc) {
		try {
			BlurClientManager.execute(connection, new BlurCommand<Void>() {
				@Override
				public Void call(Client client) {
					Calendar now = getUTCCal(new Date().getTime());
					
					Calendar twoMinutesAgo = Calendar.getInstance();
					twoMinutesAgo.setTimeInMillis(now.getTimeInMillis());
					twoMinutesAgo.add(Calendar.MINUTE, -2);
					
					jdbc.update("update blur_queries set state=1, updated_at=? where updated_at < ? and state = 0", now.getTime(), twoMinutesAgo);
					
					List<Map<String, Object>> zookeepers = jdbc.queryForList("select id from zookeepers where name = ?", zookeeperName);
					
					if (zookeepers.size() != 1) {
						log.error("Found [" + zookeepers.size() + "] zookeepers by name [" + zookeeperName + "].  Need one and only one result.  Skipping collection.");
						return null;
					}
					
					List<String> clusters = jdbc.queryForList("select name from clusters where zookeeper_id = ?", String.class, zookeepers.get(0).get("ID"));
					
					for (String clusterName : clusters) {
						List<String> tables = null;
						try {
							tables = client.tableListByCluster(clusterName);
						} catch (Exception e) {
							log.error("Unable to get table list for cluster [" + clusterName + "].  Unable to retrieve stats for tables.", e);
							return null;
						}
						
						if (tables != null) {
							for (String table : tables) {
								Map<String, Object> tableInfo = TableMap.get().get(table + "_" + clusterName);
								if (tableInfo == null) {
									log.warn("Table [" + table + "] on cluster [" + clusterName + " hasn't been loaded into datastore yet. Skipping until table shows up.");
									continue;
								}
								
								if ((Boolean) tableInfo.get("ENABLED")) {
									List<BlurQueryStatus> currentQueries = null;
									try {
										currentQueries = client.currentQueries(table);
									} catch (Exception e) {
										log.error("Unable to retrieve current queries for table [" + table + "].", e);
										continue;
									}
									
									if (currentQueries != null) {
										for (BlurQueryStatus blurQueryStatus : currentQueries) {
											//Check if query exists
											List<Map<String, Object>> existingRow = jdbc.queryForList("select id, complete_shards, times, state from blur_queries where blur_table_id=? and uuid=?", new Object[]{tableInfo.get("ID"), blurQueryStatus.getUuid()});
											
											ObjectMapper timesMapper = new ObjectMapper();
											String times = null;
											try {
												times = timesMapper.writeValueAsString(blurQueryStatus.getCpuTimes());
											} catch (Exception e) {
												log.error("Unable to parse cpu times.", e);
											}
											
											if (existingRow.isEmpty()) {
												Calendar cal = getUTCCal(new Date().getTime());
												
												SimpleQuery query = blurQueryStatus.getQuery().getSimpleQuery();
												
												Date startTime = cal.getTime();
												long startTimeLong = blurQueryStatus.getQuery().getStartTime();
												if (startTimeLong > 0) {
													Calendar startCal = getUTCCal(startTimeLong);
													startTime = startCal.getTime();
												}
												
												jdbc.update("insert into blur_queries (query_string, times, complete_shards, total_shards, state, uuid, created_at, updated_at, blur_table_id, super_query_on, facets, start, fetch_num, pre_filters, post_filters, selector_column_families, selector_columns, userid) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
															new Object[]{query.getQueryStr(), 
																times,
																blurQueryStatus.getCompleteShards(),
																blurQueryStatus.getTotalShards(),
																blurQueryStatus.getState().getValue(),
																blurQueryStatus.getUuid(),
																startTime,
																cal.getTime(),
																tableInfo.get("ID"),
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
												Calendar cal = getUTCCal(new Date().getTime());
												
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
								}
								
							}
						}
					}
					return null;
				}

				private boolean queryHasChanged(BlurQueryStatus blurQueryStatus, String timesJSON, Map<String, Object> map) {
					return blurQueryStatus.getState().getValue() == 0 || !(timesJSON.equals(map.get("TIMES")) && 
							blurQueryStatus.getCompleteShards() == (Integer)map.get("COMPLETE_SHARDS") &&
							blurQueryStatus.getState().getValue() == (Integer)map.get("STATE"));
				}
				
				private Calendar getUTCCal(long timeToStart) {
					Calendar cal = Calendar.getInstance();
					TimeZone z = cal.getTimeZone();
					cal.add(Calendar.MILLISECOND, -(z.getOffset(timeToStart)));
					return cal;
				}
			});
		} catch (Exception e) {
			log.debug(e);
		}
		
	}
	
	public static void cleanQueries(JdbcTemplate jdbc) {
		Calendar now = Calendar.getInstance();
		TimeZone z = now.getTimeZone();
		now.add(Calendar.MILLISECOND, -(z.getOffset(new Date().getTime())));
		
		Calendar twoHoursAgo = Calendar.getInstance();
		twoHoursAgo.setTimeInMillis(now.getTimeInMillis());
		twoHoursAgo.add(Calendar.HOUR_OF_DAY, -2);
		
		jdbc.update("delete from blur_queries where created_at < ?", twoHoursAgo.getTime());
	}
}
