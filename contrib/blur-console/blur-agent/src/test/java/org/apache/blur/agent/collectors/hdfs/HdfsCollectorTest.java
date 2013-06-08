package org.apache.blur.agent.collectors.hdfs;

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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.MiniCluster;
import org.apache.blur.agent.collectors.hdfs.HdfsCollector;
import org.apache.blur.agent.connections.hdfs.HdfsDatabaseConnection;
import org.apache.blur.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import org.apache.blur.agent.exceptions.HdfsThreadException;
import org.apache.blur.agent.test.BlurAgentBaseTestClass;
import org.junit.Before;
import org.junit.Test;


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