package org.apache.blur.agent.cleaners;

import org.apache.blur.agent.connections.cleaners.interfaces.HdfsDatabaseCleanerInterface;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;


public class HdfsStatsCleaner implements Runnable {
	private static final Log log = LogFactory.getLog(QueriesCleaner.class);

	private final HdfsDatabaseCleanerInterface database;

	public HdfsStatsCleaner(HdfsDatabaseCleanerInterface database) {
		this.database = database;
	}

	@Override
	public void run() {
		try {
			this.database.deleteOldStats();
		} catch (DataAccessException e) {
			log.error("An error occured while deleting hdfs stats from the database!", e);
		} catch (Exception e) {
			log.error("An unkown error occured while cleaning up the hdfs stats!", e);
		}
	}
}
