package org.apache.blur.agent.connections.cleaners;

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
import java.util.Calendar;

import org.apache.blur.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;
import org.apache.blur.agent.types.TimeHelper;
import org.springframework.jdbc.core.JdbcTemplate;


public class CleanerDatabaseConnection implements CleanerDatabaseInterface {
	private final JdbcTemplate jdbc;

	// Query Expiration Times
	private final int expireThreshold = 2 * 60 * 1000;
	private final int deleteThreshold = 2 * 60 * 60 * 1000;

	// Hdfs Expiration Times
	private final int timeToLive = 14 * 24 * 60 * 60 * 1000;

	public CleanerDatabaseConnection(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	@Override
	public int deleteOldQueries() {
		Calendar twoHoursAgo = TimeHelper.getTimeAgo(deleteThreshold);
		return this.jdbc.update("delete from blur_queries where created_at < ?", twoHoursAgo.getTime());
	}

	@Override
	public int expireOldQueries() {
		Calendar now = TimeHelper.now();
		Calendar twoMinutesAgo = TimeHelper.getTimeAgo(expireThreshold);
		return this.jdbc.update("update blur_queries set state=1, updated_at=? where updated_at < ? and state = 0", now.getTime(),
				twoMinutesAgo);
	}

	@Override
	public int deleteOldStats() {
		Calendar ttlDaysAgo = TimeHelper.getTimeAgo(timeToLive);
		return jdbc.update("delete from hdfs_stats where created_at < ?", ttlDaysAgo);
	}

}
