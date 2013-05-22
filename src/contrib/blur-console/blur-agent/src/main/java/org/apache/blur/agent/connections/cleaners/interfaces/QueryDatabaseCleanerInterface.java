package org.apache.blur.agent.connections.cleaners.interfaces;

public interface QueryDatabaseCleanerInterface {
	int deleteOldQueries();

	int expireOldQueries();

}
