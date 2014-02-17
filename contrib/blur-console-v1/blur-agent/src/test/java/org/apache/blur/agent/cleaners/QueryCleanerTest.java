package org.apache.blur.agent.cleaners;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Calendar;

import org.apache.blur.agent.cleaners.QueriesCleaner;
import org.apache.blur.agent.connections.cleaners.CleanerDatabaseConnection;
import org.apache.blur.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;
import org.apache.blur.agent.test.AgentBaseTestClass;
import org.apache.blur.agent.types.TimeHelper;
import org.junit.Test;


public class QueryCleanerTest extends AgentBaseTestClass {
	private static CleanerDatabaseInterface database = new CleanerDatabaseConnection(jdbc);

	@Test
	public void shouldExpireOldRunningQueries() {
		Calendar timeOfQuery = TimeHelper.getTimeAgo(3 * 60 * 1000);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 0, timeOfQuery, timeOfQuery);

		Thread testQueryCleaner = new Thread(new QueriesCleaner(database), "Query Test Thread");
		testQueryCleaner.start();
		try {
			testQueryCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int state = jdbc.queryForInt("select state from blur_queries limit 0, 1");
		assertEquals(1, state);
	}
	
	@Test
	public void shouldNotExpireNewlyRunningQueries() {		
		Calendar timeOfQuery = TimeHelper.getTimeAgo(1 * 60 * 1000);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 0, timeOfQuery, timeOfQuery);

		Thread testQueryCleaner = new Thread(new QueriesCleaner(database), "Query Test Thread");
		testQueryCleaner.start();
		try {
			testQueryCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int state = jdbc.queryForInt("select state from blur_queries limit 0, 1");
		assertEquals(0, state);
	}
	
	@Test
	public void shouldNotExpireExpiredRunningQueries() {		
		Calendar timeOfQuery = TimeHelper.getTimeAgo(1 * 60 * 1000);
		// a more recent time, for testing to see if a query is updated after it is created
		Calendar timeOfUpdate = TimeHelper.getTimeAgo(1 * 30 * 1000);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 1, timeOfQuery, timeOfQuery);

		Thread testQueryCleaner = new Thread(new QueriesCleaner(database), "Query Test Thread");
		testQueryCleaner.start();
		try {
			testQueryCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int updatedCount = jdbc.queryForInt("select count(id) from blur_queries where updated_at > ?", timeOfUpdate);
		assertEquals(0, updatedCount);
	}
	
	@Test
	public void shouldDeleteOldQueries() {		
		Calendar timeOfQuery = TimeHelper.getTimeAgo(3 * 60 * 60 * 1000);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 1, timeOfQuery, timeOfQuery);

		Thread testQueryCleaner = new Thread(new QueriesCleaner(database), "Query Test Thread");
		testQueryCleaner.start();
		try {
			testQueryCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int updatedCount = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(0, updatedCount);
	}
	
	@Test
	public void shouldNotDeleteYoungQueries() {		
		Calendar timeOfQuery = TimeHelper.getTimeAgo(1 * 60 * 60 * 1000);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 1, timeOfQuery, timeOfQuery);

		Thread testQueryCleaner = new Thread(new QueriesCleaner(database), "Query Test Thread");
		testQueryCleaner.start();
		try {
			testQueryCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int updatedCount = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(1, updatedCount);
	}
}