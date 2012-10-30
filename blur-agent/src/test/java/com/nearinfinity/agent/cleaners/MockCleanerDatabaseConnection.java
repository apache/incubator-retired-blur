package com.nearinfinity.agent.cleaners;

import com.nearinfinity.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;

public class MockCleanerDatabaseConnection implements CleanerDatabaseInterface {

	public int deleteStatsCalledCount = 0;
	public int deleteQueriesCalledCount = 0;
	public int expireQueriesCalledCount = 0;

	@Override
	public int deleteOldStats() {
		return ++deleteStatsCalledCount;
	}

	@Override
	public int deleteOldQueries() {
		return ++deleteQueriesCalledCount;
	}

	@Override
	public int expireOldQueries() {
		return ++expireQueriesCalledCount;
	}
}
