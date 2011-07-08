package com.nearinfinity.agent.collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.TableMap;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class TableCollector {
	public static void startCollecting(String connection, final JdbcTemplate jdbc) throws Exception {
		BlurClientManager.execute(connection, new BlurCommand<Void>() {
			@Override
			public Void call(Client client) throws Exception {
				List<String> tables = client.tableList();
				//mapper used to generate the json
 				ObjectMapper mapper = new ObjectMapper();
				
				//Create and update tables
				for (String table : tables) {				
					//TODO: This will be a problem because we aren't specifying the cluster
					List<Map<String, Object>> existingTable = jdbc.queryForList("select id from blur_tables where table_name=?", table);
					TableDescriptor descriptor = client.describe(table);
					
					//add the tablename and tableid to the map that acts as a dictionary
					if (!existingTable.isEmpty()){
						TableMap.get().put(table, (Integer)(existingTable.get(0).get("id")));
					}
					
					//strings that are being mocked to json
					Schema schema = client.schema(table);
					String schemaString = mapper.writeValueAsString(schema);
					
					Map<String, String> shardServerLayout = client.shardServerLayout(table);
					
					Map<String, ArrayList<String>> formattedShard = new HashMap<String, ArrayList<String>>();
					for(String shard : shardServerLayout.keySet()){
						String host = shardServerLayout.get(shard);
						if(formattedShard.get(host) != null){
							formattedShard.get(host).add(shard);
						} else {
							formattedShard.put(host, new ArrayList<String>(Arrays.asList(shard)));
						}
					}
					
					String shardServerString = mapper.writeValueAsString(formattedShard);
					
					//other relevant data to be inserted
//					String tableUri = descriptor.tableUri;
					String tableAnalyzer = descriptor.analyzerDefinition.fullTextAnalyzerClassName;
					
					
					//TODO: need to pull stats
					
					if (existingTable.isEmpty()) {
						//New Table
//						jdbc.update("insert into blur_tables (table_name, status, table_uri, table_analyzer, table_schema, server) values (?, ?, ?, ?, ?, ?)", 
//								new Object[]{table, descriptor.isIsEnabled() ? 2 : 1, tableUri, tableAnalyzer, schemaString, shardServerString});
					} else {
						//Update Table
						jdbc.update("update blur_tables set table_analyzer=?, table_schema=?, server=? where table_name=?", 
								new Object[]{tableAnalyzer, schemaString, shardServerString, table});
					}
				}
				
				return null;
			}
		});
	}
}
