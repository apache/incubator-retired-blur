package com.nearinfinity;

import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.JdbcConnection;

public abstract class AgentBaseTestClass {
	protected static JdbcTemplate jdbc;

	@BeforeClass
	public static void setupDatabaseConnection() {
		Properties props = new Properties();
		props.setProperty("store.url", "jdbc:mysql://localhost/blurtools-test");
		props.setProperty("store.user", "root");
		props.setProperty("store.password", "");
		jdbc = JdbcConnection.createDBConnection(props);
	}

	@After
	public void tearDownDatabase() {
		List<String> tables = jdbc.queryForList("select TABLE_NAME from information_schema.tables where table_schema = 'blurtools-test'",
				String.class);

		for (String table : tables) {
			if (!"schema_migrations".equalsIgnoreCase(table)) {
				jdbc.execute("truncate table " + table);
			}
		}
	}
}
