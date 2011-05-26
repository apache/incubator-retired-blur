package com.nearinfinity.agent.collectors;

import java.util.List;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;

public class QueryCollector {
	public static void startCollecting(String connection) {
		try {
			BlurClientManager.execute(connection, new BlurCommand<Void>() {
				@Override
				public Void call(Client client) throws Exception {
					List<String> tables = client.tableList();
					
					System.out.println(tables);
					
					/**
					 *  
					    +----------------+--------------+------+-----+---------+----------------+
						| Field          | Type         | Null | Key | Default | Extra          |
						+----------------+--------------+------+-----+---------+----------------+
						| id             | int(11)      | NO   | PRI | NULL    | auto_increment |
						| query_string   | varchar(255) | YES  |     | NULL    |                |
						| cpu_time       | int(11)      | YES  |     | NULL    |                |
						| real_time      | int(11)      | YES  |     | NULL    |                |
						| complete       | int(11)      | YES  |     | NULL    |                |
						| interrupted    | tinyint(1)   | YES  |     | NULL    |                |
						| running        | tinyint(1)   | YES  |     | NULL    |                |
						| uuid           | varchar(255) | YES  |     | NULL    |                |
						| created_at     | datetime     | YES  |     | NULL    |                |
						| updated_at     | datetime     | YES  |     | NULL    |                |
						| table_name     | varchar(255) | YES  |     | NULL    |                |
						| super_query_on | tinyint(1)   | YES  |     | NULL    |                |
						| facets         | varchar(255) | YES  |     | NULL    |                |
						| selectors      | varchar(255) | YES  |     | NULL    |                |
						| start          | int(11)      | YES  |     | NULL    |                |
						| fetch          | int(11)      | YES  |     | NULL    |                |
						+----------------+--------------+------+-----+---------+----------------+

					 */
					
					for (String table : tables) {
						List<BlurQueryStatus> currentQueries = client.currentQueries(table);
						
						System.out.println(currentQueries);
					}
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
