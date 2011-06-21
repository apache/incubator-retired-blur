package com.nearinfinity.agent.collectors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class TableCollector {
	public static void startCollecting(String connection, final JdbcTemplate jdbc) throws Exception {
		/**
		 * 
					+----------------+--------------+------+-----+---------+----------------+
					| Field          | Type         | Null | Key | Default | Extra          |
					+----------------+--------------+------+-----+---------+----------------+
					| id             | int(11)      | NO   | PRI | NULL    | auto_increment |
					| table_name     | varchar(255) | YES  |     | NULL    |                |
					| current_size   | int(11)      | YES  |     | NULL    |                |
					| query_usage    | int(11)      | YES  |     | NULL    |                |
					| created_at     | datetime     | YES  |     | NULL    |                |
					| updated_at     | datetime     | YES  |     | NULL    |                |
					| record_count   | bigint(20)   | YES  |     | NULL    |                |
					| status         | int(11)      | YES  |     | NULL    |                |
					| table_uri      | varchar(255) | YES  |     | NULL    |                |
					| table_analyzer | text         | YES  |     | NULL    |                |
					| schema         | text         | YES  |     | NULL    |                |
					| server         | text         | YES  |     | NULL    |                |
					+----------------+--------------+------+-----+---------+----------------+

		 */
		
		BlurClientManager.execute(connection, new BlurCommand<Void>() {
			@Override
			public Void call(Client client) throws Exception {
				List<String> tables = client.tableList();
				//mapper used to generate the json
				ObjectMapper mapper = new ObjectMapper();
				
				//Mark deleted tables deleted
				jdbc.update("update blur_tables set status = 0 where table_name not in ('" + StringUtils.join(tables, "','") + "')");
				
				//Create and update tables
				for (String table : tables) {
					List<Map<String, Object>> existingTable = jdbc.queryForList("select id from blur_tables where table_name=?", table);
					TableDescriptor descriptor = client.describe(table);
					
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
					String tableUri = descriptor.tableUri;
					String tableAnalyzer = descriptor.analyzerDefinition.fullTextAnalyzerClassName;
					
					
					//TODO: need to pull stats
					
					if (existingTable.isEmpty()) {
						//New Table
						jdbc.update("insert into blur_tables (table_name, status, table_uri, table_analyzer, table_schema, server) values (?, ?, ?, ?, ?, ?)", 
								new Object[]{table, descriptor.isIsEnabled() ? 2 : 1, tableUri, tableAnalyzer, schemaString, shardServerString});
					} else {
						//Update Table
						jdbc.update("update blur_tables set status=?, table_uri=?, table_analyzer=?, table_schema=?, server=? where table_name=?", 
								new Object[]{descriptor.isIsEnabled() ? 2 : 1, tableUri, tableAnalyzer, schemaString, shardServerString, table});
					}
				}
				
				return null;
			}
		});
	}
}
