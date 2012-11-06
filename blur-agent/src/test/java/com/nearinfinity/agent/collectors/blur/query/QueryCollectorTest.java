package com.nearinfinity.agent.collectors.blur.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.Test;

import com.nearinfinity.BlurAgentBaseTestClass;
import com.nearinfinity.agent.connections.blur.BlurDatabaseConnection;
import com.nearinfinity.blur.MiniCluster;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class QueryCollectorTest extends BlurAgentBaseTestClass {
	private static BlurDatabaseConnection database = new BlurDatabaseConnection(jdbc);

	@Test
	public void shouldAddQueriesToDB() throws BlurException, TException, IOException {
		System.out.println(MiniCluster.getControllerConnectionStr());
		Iface blurConnection = BlurClient.getClient(MiniCluster.getControllerConnectionStr());


		TableDescriptor td = new TableDescriptor(); 
		td.setTableUri(MiniCluster.getFileSystemUri() + "/blur-tables/test-table");
		td.setAnalyzerDefinition(new AnalyzerDefinition());
		td.setName("test");

		blurConnection.createTable(td);
		
		blurConnection.query("test.col:*", new BlurQuery());
		
		Thread testQueryCollector = new Thread(new QueryCollector(BlurClient.getClient(MiniCluster.getControllerConnectionStr()), "test",
				1, database), "Query Test Thread");
		testQueryCollector.start();
		try {
			testQueryCollector.join();
		} catch (InterruptedException e) {
			fail("The test QueryCollector failed while waiting for it to finish!");
		}
		int query_count = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(1, query_count);
	}
}
