package com.nearinfinity.agent.collectors;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

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
					List<String> tables = client.tableList();
					
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
						| pre_filters    | mediumtext   | YES  |     | NULL    |                |
						| post_filters   | mediumtext   | YES  |     | NULL    |                |
						+----------------+--------------+------+-----+---------+----------------+

					 */
					
					for (String table : tables) {
						List<BlurQueryStatus> currentQueries = client.currentQueries(table);
						
						for (BlurQueryStatus blurQueryStatus : currentQueries) {
							//Check if query exists
							List<Map<String, Object>> existingRow = jdbc.queryForList("select id, complete from blur_queries where table_name=? and uuid=?", new Object[]{table, blurQueryStatus.getUuid()});
							
							if (existingRow.isEmpty()) {
								System.out.println("Inserting new query");
								jdbc.update("insert into blur_queries (query_string, cpu_time, real_time, complete, interrupted, running, uuid, created_at, table_name, super_query_on, facets, start, fetch_num, pre_filters, post_filters) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
											new Object[]{blurQueryStatus.getQuery().getQueryStr(), 
												blurQueryStatus.getCpuTime(),
												blurQueryStatus.getRealTime(),
												blurQueryStatus.getComplete(),
												blurQueryStatus.isInterrupted(),
												blurQueryStatus.isRunning(),
												blurQueryStatus.getUuid(),
												new Date(),
												table,
												blurQueryStatus.getQuery().isSuperQueryOn(),
												StringUtils.join(blurQueryStatus.getQuery().getFacets(), ", "),
												//TODO: make this better
//												blurQueryStatus.getQuery().getSelector().toString(),
												blurQueryStatus.getQuery().getStart(),
												blurQueryStatus.getQuery().getFetch(),
												blurQueryStatus.getQuery().getPreSuperFilter(),
												blurQueryStatus.getQuery().getPostSuperFilter()
											});
							} else if ((Integer)existingRow.get(0).get("COMPLETE") != 1){
								System.out.println("Updating existing query");
								jdbc.update("update blur_queries set cpu_time=?, real_time=?, complete=?, interrupted=?, running=? where id=?", 
											new Object[] {blurQueryStatus.getCpuTime(),
												blurQueryStatus.getRealTime(),
												blurQueryStatus.getComplete(),
												blurQueryStatus.isInterrupted(),
												blurQueryStatus.isRunning(),
												existingRow.get(0).get("ID")
											});
							}
						}
						
					}
					return null;
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
