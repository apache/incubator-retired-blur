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

import org.apache.blur.agent.cleaners.HdfsStatsCleaner;
import org.apache.blur.agent.connections.cleaners.CleanerDatabaseConnection;
import org.apache.blur.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;
import org.apache.blur.agent.test.AgentBaseTestClass;
import org.apache.blur.agent.types.TimeHelper;
import org.junit.Test;


public class HdfsStatsCleanerTest extends AgentBaseTestClass {
	private static CleanerDatabaseInterface database = new CleanerDatabaseConnection(jdbc);

	@Test
	public void shouldDeleteOldStats() {
		Calendar overTwoWeeksAgo = TimeHelper.getTimeAgo(16 * 24 * 60 * 60 * 1000);
		jdbc.update("insert into hdfs_stats (created_at) values (?)", overTwoWeeksAgo);

		Thread testStatsCleaner = new Thread(new HdfsStatsCleaner(database), "Hdfs Stats Cleaner Test Thread");
		testStatsCleaner.start();
		try {
			testStatsCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int updatedCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(0, updatedCount);
	}
	
	@Test
	public void shouldNotDeleteYoungStats() {
		Calendar underTwoWeeksAgo = TimeHelper.getTimeAgo(8 * 24 * 60 * 60 * 1000);
		jdbc.update("insert into hdfs_stats (created_at) values (?)", underTwoWeeksAgo);

		Thread testStatsCleaner = new Thread(new HdfsStatsCleaner(database), "Hdfs Stats Cleaner Test Thread");
		testStatsCleaner.start();
		try {
			testStatsCleaner.join();
		} catch (InterruptedException e) {
			fail("The test QueriesCleaner failed while waiting for it to finish!");
		}

		int updatedCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(1, updatedCount);
	}
}
