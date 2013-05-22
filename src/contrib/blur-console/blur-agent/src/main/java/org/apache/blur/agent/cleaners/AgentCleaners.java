package org.apache.blur.agent.cleaners;

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
