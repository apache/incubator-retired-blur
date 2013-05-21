package com.nearinfinity.agent.collectors.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.MiniCluster;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.BlurAgentBaseTestClass;
import com.nearinfinity.agent.connections.hdfs.HdfsDatabaseConnection;
import com.nearinfinity.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import com.nearinfinity.agent.exceptions.HdfsThreadException;

public class HdfsCollectorTest extends BlurAgentBaseTestClass {
	private static HdfsDatabaseInterface database = new HdfsDatabaseConnection(jdbc);
	private static URI hdfsUri;

	@Before
	public void setup() throws IOException {
		hdfsUri = MiniCluster.getFileSystemUri();
	}

	@Test
	public void shouldCreateHdfs() throws HdfsThreadException {
		List<String> activeCollectors = new ArrayList<String>();

		Thread testHdfsCollector = new Thread(new HdfsCollector("TestHDFS", hdfsUri.toString(), "hdfs://localhost:55314", null,
				activeCollectors, database), "Hdfs Test Thread");
		testHdfsCollector.start();
		waitForThreadToSleep(testHdfsCollector, 250);

		int hdfsCount = jdbc.queryForInt("select count(id) from hdfs");
		assertEquals(1, hdfsCount);
	}

	@Test
	public void shouldCollectStatsWhenActive() throws HdfsThreadException {
		List<String> activeCollectors = new ArrayList<String>();
		activeCollectors.add("hdfs");

		Thread testHdfsCollector = new Thread(new HdfsCollector("TestHDFS", hdfsUri.toString(), "hdfs://localhost:55314", null,
				activeCollectors, database), "Hdfs Test Thread");
		testHdfsCollector.start();
		waitForThreadToSleep(testHdfsCollector, 250);

		int statsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(1, statsCount);
	}

	@Test
	public void shouldNotCollectStatsWithoutActive() throws HdfsThreadException {
		List<String> activeCollectors = new ArrayList<String>();

		Thread testHdfsCollector = new Thread(new HdfsCollector("TestHDFS", hdfsUri.toString(), "hdfs://localhost:55314", null,
				activeCollectors, database), "Hdfs Test Thread");
		testHdfsCollector.start();
		waitForThreadToSleep(testHdfsCollector, 250);

		int statsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(0, statsCount);
	}
}