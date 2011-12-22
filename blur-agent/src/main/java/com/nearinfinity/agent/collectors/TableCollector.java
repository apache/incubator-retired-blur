package com.nearinfinity.agent.collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.TableMap;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;

public class TableCollector {
	private static final Log log = LogFactory.getLog(TableCollector.class);
	
	public static void startCollecting(String connection, final JdbcTemplate jdbc) {
		try {
			BlurClientManager.execute(connection, new BlurCommand<Void>() {
				@Override
				public Void call(Client client) {
					List<String> tables = null;
					try {
						tables = client.tableList();
					} catch (Exception e) {
						log.error("Unable to get table list from blur. Unable to retrive stats for tables.", e);
					}
					
					if (tables != null) {
						//mapper used to generate the json
						ObjectMapper mapper = new ObjectMapper();
						
						//Create and update tables
						for (String table : tables) {				
							TableDescriptor descriptor = null;
							try {
								descriptor = client.describe(table);
							} catch (Exception e) {
								log.error("Unable to describe table [" + table + "].", e);
								continue;
							}
							
							if (descriptor != null) {
								Integer clusterId = jdbc.queryForInt("select id from clusters where name=?", new Object[]{descriptor.getCluster()});
								
								List<Map<String, Object>> existingTable = jdbc.queryForList("select id, cluster_id from blur_tables where table_name=? and cluster_id=?", table, clusterId);
								
								//add the tablename and tableid to the map that acts as a dictionary
								if (!existingTable.isEmpty()){
									TableMap.get().put(table, (Integer)(existingTable.get(0).get("id")));
								}
								
								if (descriptor.isEnabled) {
									//strings that are being mocked to json
									Schema schema = null;
									try {
										schema = client.schema(table);
									} catch (Exception e) {
										log.error("Unable to get schema for table [" + table + "].", e);
									}
									
									String schemaString = "";
									if (schema != null) {
										try {
											schemaString = mapper.writeValueAsString(schema);
										} catch (Exception e) {
											log.error("Unable to convert schema to json.", e);
										}
										
									}
									
									Map<String, String> shardServerLayout = null;
									try {
										shardServerLayout = client.shardServerLayout(table);
									} catch (Exception e) {
										log.error("Unable to get shard server layout for table [" + table +"].", e);
									}
									
									String shardServerString = "";
									if (shardServerLayout != null) {
										Map<String, ArrayList<String>> formattedShard = new HashMap<String, ArrayList<String>>();
										for(String shard : shardServerLayout.keySet()){
											String host = shardServerLayout.get(shard);
											if(formattedShard.get(host) != null){
												formattedShard.get(host).add(shard);
											} else {
												formattedShard.put(host, new ArrayList<String>(Arrays.asList(shard)));
											}
										}
										
										try {
											shardServerString = mapper.writeValueAsString(formattedShard);
										} catch (Exception e) {
											log.error("Unable to convert shard server layout to json.", e);
										}
									}
									String tableAnalyzer = descriptor.analyzerDefinition.fullTextAnalyzerClassName;
									
									TableStats tableStats = null;
									Long tableBytes = null;
									Long tableQueries = null;
									Long tableRecordCount = null;
									Long tableRowCount = null;
									try {
										tableStats = client.getTableStats(table);
										if (tableStats != null) {
											tableBytes = tableStats.getBytes();
											tableQueries = tableStats.getQueries();
											tableRecordCount = tableStats.getRecordCount();
											tableRowCount = tableStats.getRowCount();
										}
									} catch (Exception e) {
										log.error("Unable to get table stats for table [" + table + "].", e);
									}
									
									if (!existingTable.isEmpty()) {
										//Update Table
										jdbc.update("update blur_tables set table_analyzer=?, table_schema=?, server=?, current_size=?, query_usage=?, record_count=?, row_count=? where table_name=? and cluster_id=?", 
												new Object[]{tableAnalyzer, schemaString, shardServerString, tableBytes, tableQueries, tableRecordCount, tableRowCount, table, clusterId});
									}
								}
							}
						}
					}
					
					return null;
				}
			});
		} catch (Exception e) {
			log.debug(e);
		}
	}
}
