package com.nearinfinity;

import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.JdbcConnection;
import com.nearinfinity.blur.MiniCluster;

public abstract class BlurAgentBaseTestClass {
	protected static JdbcTemplate jdbc;

	@BeforeClass
	public static void setupBlur() {
		// MiniCluster.startDfs("./tmp");
		// MiniCluster.startZooKeeper("./tmp");
		//
		// MiniCluster.startControllers(1);
		// MiniCluster.startShards(1);

		Properties props = new Properties();
		props.setProperty("store.url", "jdbc:mysql://localhost/blurtools-test");
		props.setProperty("store.user", "root");
		props.setProperty("store.password", "");
		jdbc = JdbcConnection.createDBConnection(props);
	}

	@AfterClass
	public static void tearDownBlur() {
		// MiniCluster.stopShards();
		// MiniCluster.stopControllers();
		//
		// MiniCluster.shutdownZooKeeper();
		// MiniCluster.shutdownDfs();
	}

	@Before
	public void setup() {

	}

	@After
	public void tearDown() {
		List<String> tables = jdbc.queryForList("select TABLE_NAME from information_schema.tables where table_schema = 'blurtools-test'",
				String.class);

		for (String table : tables) {
			if (!"schema_migrations".equalsIgnoreCase(table)) {
				jdbc.execute("truncate table " + table);
			}
		}
	}
}
