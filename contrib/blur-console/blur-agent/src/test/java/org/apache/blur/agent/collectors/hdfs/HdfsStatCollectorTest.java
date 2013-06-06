package org.apache.blur.agent.collectors.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.blur.MiniCluster;
import org.apache.blur.agent.connections.hdfs.HdfsDatabaseConnection;
import org.apache.blur.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import org.apache.blur.agent.test.BlurAgentBaseTestClass;
import org.junit.Before;
import org.junit.Test;


public class HdfsStatCollectorTest extends BlurAgentBaseTestClass {
	private static HdfsDatabaseInterface database = new HdfsDatabaseConnection(jdbc);
	private static URI hdfsUri;

	@Before
	public void setup() throws IOException {
		hdfsUri = MiniCluster.getFileSystemUri();
	}

	@Test
	public void shouldNotInsertStatsWithoutParent() {
		Thread testHdfsStatsCollector = new Thread(new HdfsStatsCollector("Test HDFS", hdfsUri, null, database), "HdfsStat Test Thread");
		testHdfsStatsCollector.start();
		try {
			testHdfsStatsCollector.join();
		} catch (InterruptedException e) {
			fail("The test HdfsStatCollector failed while waiting for it to finish!");
		}

		int statsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(0, statsCount);
	}
	
	@Test
	public void shouldInsertStats() {
		jdbc.update("insert into hdfs (name) values (?)", "TestHdfs");
		Thread testHdfsStatsCollector = new Thread(new HdfsStatsCollector("TestHdfs", hdfsUri, null, database), "HdfsStat Test Thread");
		testHdfsStatsCollector.start();
		try {
			testHdfsStatsCollector.join();
		} catch (InterruptedException e) {
			fail("The test HdfsStatCollector failed while waiting for it to finish!");
		}

		int statsCount = jdbc.queryForInt("select count(id) from hdfs_stats");
		assertEquals(1, statsCount);
	}
}
