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
import java.util.List;

import org.apache.blur.agent.Agent;
import org.apache.blur.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;


public class AgentCleaners implements Runnable {

	private final boolean cleanQueries;
	private final boolean cleanHdfsStats;
	private final CleanerDatabaseInterface database;

	public AgentCleaners(final List<String> activeCollectors, CleanerDatabaseInterface database) {
		this.cleanQueries = activeCollectors.contains("queries");
		this.cleanHdfsStats = activeCollectors.contains("hdfs");
		this.database = database;
	}

	@Override
	public void run() {
		while (true) {
			if (this.cleanQueries) {
				new Thread(new QueriesCleaner(this.database), "Query Cleaner").start();
			}

			if (this.cleanHdfsStats) {
				new Thread(new HdfsStatsCleaner(this.database), "Hdfs Stats Cleaner").start();
			}

			try {
				Thread.sleep(Agent.CLEAN_UP_SLEEP_TIME);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

}
