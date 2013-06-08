package org.apache.blur.agent.test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.List;
import java.util.Properties;

import org.apache.blur.agent.connections.JdbcConnection;
import org.junit.After;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;


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
	
	protected void waitForThreadToSleep(Thread tiredThread, int catchupTime){
		while(tiredThread.getState() != Thread.State.TIMED_WAITING){
			// Wait until the thread goes to sleep
		}
		try {
			Thread.sleep(catchupTime);
			tiredThread.interrupt();
		} catch (InterruptedException e) {}
		return;
	}
}
