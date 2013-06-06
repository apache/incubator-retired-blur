package org.apache.blur.agent.cleaners;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.blur.agent.cleaners.AgentCleaners;
import org.apache.blur.agent.connections.cleaners.CleanerDatabaseConnection;
import org.apache.blur.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;
import org.apache.blur.agent.test.AgentBaseTestClass;
import org.apache.blur.agent.types.TimeHelper;
import org.junit.Test;


public class AgentCleanerTest extends AgentBaseTestClass {
	private static CleanerDatabaseInterface database = new CleanerDatabaseConnection(jdbc);

	@Test
	public void shouldCleanStatsAndQueries() {
		List<String> activeCollectors = new ArrayList<String>();
		activeCollectors.addAll(Arrays.asList("queries", "hdfs"));

		makeOldData();

		Thread testStatsCleaner = new Thread(new AgentCleaners(activeCollectors, database), "Test Agent Thread");
		testStatsCleaner.start();
		waitForThreadToSleep(testStatsCleaner, 250);

		int hdfsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		int queryCount = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(0, hdfsCount);
		assertEquals(0, queryCount);
	}

	@Test
	public void shouldOnlyCleanStats() {
		List<String> activeCollectors = new ArrayList<String>();
		activeCollectors.add("hdfs");

		makeOldData();

		Thread testStatsCleaner = new Thread(new AgentCleaners(activeCollectors, database), "Test Agent Thread");
		testStatsCleaner.start();
		waitForThreadToSleep(testStatsCleaner, 250);

		int hdfsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		int queryCount = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(0, hdfsCount);
		assertEquals(1, queryCount);
	}

	@Test
	public void shouldOnlyCleanQueries() {
		List<String> activeCollectors = new ArrayList<String>();
		activeCollectors.add("queries");

		makeOldData();

		Thread testStatsCleaner = new Thread(new AgentCleaners(activeCollectors, database), "Test Agent Thread");
		testStatsCleaner.start();
		waitForThreadToSleep(testStatsCleaner, 250);

		int hdfsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		int queryCount = jdbc.queryForInt("select count(id) from blur_queries");
		assertEquals(1, hdfsCount);
		assertEquals(0, queryCount);
	}

	private void makeOldData() {
		Calendar overTwoWeeksAgo = TimeHelper.getTimeAgo(16 * 24 * 60 * 60 * 1000);
		jdbc.update("insert into hdfs_stats (created_at) values (?)", overTwoWeeksAgo);
		jdbc.update("insert into blur_queries (state, updated_at, created_at) values (?,?,?)", 0, overTwoWeeksAgo, overTwoWeeksAgo);
	}
}
