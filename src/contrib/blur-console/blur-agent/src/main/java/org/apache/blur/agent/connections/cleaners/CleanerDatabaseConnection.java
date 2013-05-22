package org.apache.blur.agent.connections.cleaners;

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
