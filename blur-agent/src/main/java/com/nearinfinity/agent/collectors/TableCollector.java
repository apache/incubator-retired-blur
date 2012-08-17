package com.nearinfinity.agent.collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.TableMap;
import com.nearinfinity.agent.collectors.model.Column;
import com.nearinfinity.agent.collectors.model.Family;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.ColumnDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnFamilyDefinition;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;

public class TableCollector {
	private static final Log log = LogFactory.getLog(TableCollector.class);
	
	public static void startCollecting(String connection, final String zookeeperName, final JdbcTemplate jdbc) {
		log.debug("Collecting table info");
		try {
			List<Map<String, Object>> zookeepers = jdbc.queryForList("select id from zookeepers where name = ?", zookeeperName);
			
			if (zookeepers.size() != 1) {
				log.error("Found [" + zookeepers.size() + "] zookeepers by name [" + zookeeperName + "].  Need one and only one result.  Skipping collection.");
				return;
			}
			
			List<Map<String, Object>> clusters = jdbc.queryForList("select id, name from clusters where zookeeper_id = ?", zookeepers.get(0).get("ID"));
			
			for (Map<String, Object> cluster : clusters) {
				final String clusterName = (String) cluster.get("NAME");
				Integer clusterId = (Integer) cluster.get("ID");
				List<String> tables = null;
				try {
					tables = BlurClientManager.execute(connection, new BlurCommand<List<String>>() {
						@Override
						public List<String> call(Client client) throws BlurException, TException {
							return client.tableListByCluster(clusterName);
						}
					});
				} catch (Exception e) {
					log.error("Unable to get table list for cluster [" + clusterName + "].  Unable to retrieve stats for tables.", e);
					return;
				}
				
				if (tables != null) {
					//mapper used to generate the json
					ObjectMapper mapper = new ObjectMapper();
					
					//Create and update tables
					for (final String table : tables) {				
						TableDescriptor descriptor = null;
						try {
							descriptor = BlurClientManager.execute(connection, new BlurCommand<TableDescriptor>() {
								@Override
								public TableDescriptor call(Client client) throws BlurException, TException {
									return client.describe(table);
								}
							});
						} catch (Exception e) {
							log.error("Unable to describe table [" + table + "].", e);
							continue;
						}
						
						if (descriptor != null) {
							List<Map<String, Object>> existingTable = jdbc.queryForList("select id, cluster_id from blur_tables where table_name=? and cluster_id=?", table, clusterId);
							
							//add the tablename and tableid to the map that acts as a dictionary
							if (!existingTable.isEmpty()){
								Map<String, Object> tableInfo = new HashMap<String, Object>();
								tableInfo.put("ID", existingTable.get(0).get("id"));
								tableInfo.put("ENABLED", descriptor.isEnabled);
								TableMap.get().put(table + "_" + clusterName, tableInfo);
							}
							
							System.out.println("Table: [" + table + "] Enabled: " + descriptor.isEnabled );
							
							if (descriptor.isEnabled) {
								//strings that are being mocked to json
								Schema schema = null;
								try {
									schema = BlurClientManager.execute(connection, new BlurCommand<Schema>() {
										@Override
										public Schema call(Client client) throws BlurException, TException {
											return client.schema(table);
										}
									});
								} catch (Exception e) {
									log.error("Unable to get schema for table [" + table + "].", e);
								}
								
								String schemaString = "";
								if (schema != null && descriptor != null) {
									List<Family> columnDefs = new ArrayList<Family>();
									Map<String, Set<String>> columnFamilies = schema.getColumnFamilies();
									if (columnFamilies != null) {
										for (Map.Entry<String, Set<String>> schemaEntry : columnFamilies.entrySet()) {
											Family family = new Family(schemaEntry.getKey());
											for (String columnName : schemaEntry.getValue()) {
												Column column = new Column(columnName);
												column.setLive(true);
												family.getColumns().add(column);
											}
											columnDefs.add(family);
										}
									}
									
									AnalyzerDefinition analyzerDefinition = descriptor.getAnalyzerDefinition();
									if (analyzerDefinition != null) {
										Map<String, ColumnFamilyDefinition> columnFamilyDefinitions = analyzerDefinition.getColumnFamilyDefinitions();
										ColumnDefinition analyzerDefaultDefinition = analyzerDefinition.getDefaultDefinition();
										if (columnFamilyDefinitions == null) {
											for (Family family : columnDefs) {
												for (Column column : family.getColumns()) {
													if (analyzerDefaultDefinition == null) {
														column.setAnalyzer("UNKNOWN");
													} else {
														column.setAnalyzer(analyzerDefaultDefinition.getAnalyzerClassName());
														column.setFullText(analyzerDefaultDefinition.isFullTextIndex());
													}
												}
											}
										} else {
											for (Map.Entry<String, ColumnFamilyDefinition> describeEntry : columnFamilyDefinitions.entrySet()) {
												Family family = new Family(describeEntry.getKey());
												int familyIndex = columnDefs.indexOf(family);
												
												if (familyIndex == -1) {
													columnDefs.add(family);
												} else {
													family = columnDefs.get(familyIndex);
												}
												
												Map<String, ColumnDefinition> columnDefinitions = describeEntry.getValue().getColumnDefinitions();
												ColumnDefinition columnDefaultDefinition = describeEntry.getValue().getDefaultDefinition();
												if (columnDefinitions == null) {
													for (Column column : family.getColumns()) {
														if (columnDefaultDefinition == null && analyzerDefaultDefinition == null) {
															column.setAnalyzer("UNKNOWN");
														} else if (columnDefaultDefinition == null) {
															column.setAnalyzer(analyzerDefaultDefinition.getAnalyzerClassName());
															column.setFullText(analyzerDefaultDefinition.isFullTextIndex());
														} else {
															column.setAnalyzer(columnDefaultDefinition.getAnalyzerClassName());
															column.setFullText(columnDefaultDefinition.isFullTextIndex());
														}
													}
												} else {
													for (Map.Entry<String, ColumnDefinition> columnDescription : columnDefinitions.entrySet()) {
														Column column = new Column(columnDescription.getKey());
														int columnIndex = family.getColumns().indexOf(column);
														
														if (columnIndex == -1) {
															family.getColumns().add(column);
														} else {
															column = family.getColumns().get(columnIndex);
														}
														
														column.setAnalyzer(columnDescription.getValue().getAnalyzerClassName());
														column.setFullText(columnDescription.getValue().isFullTextIndex());
													}
												}									
											}
										}
									}
									
									try {
										schemaString = mapper.writeValueAsString(columnDefs);
									} catch (Exception e) {
										log.error("Unable to convert column definition to json.", e);
									}
								}
								
								Map<String, String> shardServerLayout = null;
								try {
									shardServerLayout = BlurClientManager.execute(connection, new BlurCommand<Map<String, String>>() {
										@Override
										public Map<String, String> call(Client client) throws BlurException, TException {
											return client.shardServerLayout(table);
										}
									});
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
								String tableAnalyzer = descriptor.getAnalyzerDefinition().getFullTextAnalyzerClassName();
								
								TableStats tableStats = null;
								Long tableBytes = null;
								Long tableQueries = null;
								Long tableRecordCount = null;
								Long tableRowCount = null;
								try {
									tableStats = BlurClientManager.execute(connection, new BlurCommand<TableStats>() {
										@Override
										public TableStats call(Client client) throws BlurException, TException {
											return client.getTableStats(table);
										}
									});
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
			}
		} catch (Exception e) {
			log.warn("Got an error while collecting tables.  Will try again next pass.", e);
		}
	}
}
