package com.nearinfinity.agent.collectors;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
		 	+--------------+--------------+------+-----+---------+----------------+
			| Field        | Type         | Null | Key | Default | Extra          |
			+--------------+--------------+------+-----+---------+----------------+
			| id           | int(11)      | NO   | PRI | NULL    | auto_increment |
			| table_name   | varchar(255) | YES  |     | NULL    |                |
			| current_size | int(11)      | YES  |     | NULL    |                |
			| query_usage  | int(11)      | YES  |     | NULL    |                |
			| created_at   | datetime     | YES  |     | NULL    |                |
			| updated_at   | datetime     | YES  |     | NULL    |                |
			| record_count | bigint(20)   | YES  |     | NULL    |                |
			| status       | tinyint(1)   | YES  |     | NULL    |                |
			+--------------+--------------+------+-----+---------+----------------+

		 */
		
		BlurClientManager.execute(connection, new BlurCommand<Void>() {
			@Override
			public Void call(Client client) throws Exception {
				List<String> tables = client.tableList();
				
				//Mark deleted tables deleted
				jdbc.update("update blur_tables set status = 0 where table_name not in ('" + StringUtils.join(tables, "','") + "')");
				
				//Create and update tables
				for (String table : tables) {
					List<Map<String, Object>> existingTable = jdbc.queryForList("select id from blur_tables where table_name=?", table);
					TableDescriptor descriptor = client.describe(table);
					Schema schema = client.schema(table);
					Map<String, String> shardServerLayout = client.shardServerLayout(table);
					//TODO: need to pull stats
					
					if (existingTable.isEmpty()) {
						//New Table
						jdbc.update("insert into blur_tables (table_name, status) values (?, ?)", new Object[]{table, descriptor.isIsEnabled() ? 2 : 1});
					} else {
						//Update Table
						jdbc.update("update blur_tables set status=? where table_name=?", new Object[]{descriptor.isIsEnabled() ? 2 : 1, table});
					}
				}
				
				return null;
			}
		});
	}
}
