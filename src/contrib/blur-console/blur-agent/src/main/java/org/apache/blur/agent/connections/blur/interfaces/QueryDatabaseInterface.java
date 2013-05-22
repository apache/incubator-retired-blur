package org.apache.blur.agent.connections.blur.interfaces;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.SimpleQuery;

public interface QueryDatabaseInterface {
	Map<String, Object> getQuery(int tableId, long UUID);

	List<Long> getRunningQueries();

	void createQuery(BlurQueryStatus status, SimpleQuery query, String times, Date startTime, int tableId);

	void updateQuery(BlurQueryStatus status, String times, int queryId);
	
	void markOrphanedRunningQueriesComplete(Collection<Long> queries);
}
