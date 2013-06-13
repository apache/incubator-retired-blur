package org.apache.blur.agent.collectors.blur.query;

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
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.blur.agent.connections.blur.interfaces.QueryDatabaseInterface;
import org.apache.blur.agent.types.TimeHelper;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.SimpleQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;


public class QueryCollector implements Runnable {
	private static final Log log = LogFactory.getLog(QueryCollector.class);

	private final Iface blurConnection;
	private final String tableName;
	private final int tableId;
	private final QueryDatabaseInterface database;

	public QueryCollector(Iface connection, String tableName, int tableId, QueryDatabaseInterface database) {
		this.blurConnection = connection;
		this.tableName = tableName;
		this.tableId = tableId;
		this.database = database;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		Set<Long> currentQueries = new HashSet<Long>();
		try {
			currentQueries.addAll(blurConnection.queryStatusIdList(tableName));
			//currentQueries.addAll(this.database.getRunningQueries());
		} catch (Exception e) {
			log.error("Unable to get the list of current queries [" + tableName + "]." + e.getMessage());
			return;
		}
		
		// Mark running queries that can't be found as complete - unknown
		this.database.markOrphanedRunningQueriesComplete(CollectionUtils.subtract(this.database.getRunningQueries((long)tableId), currentQueries));
		

		for (Long queryUUID : currentQueries) {
			BlurQueryStatus status;
			try {
				status = blurConnection.queryStatusById(tableName, queryUUID);
			} catch (Exception e) {
				log.error("Unable to get query status for query [" + queryUUID + "]." + e.getMessage());
				continue;
			}

			Map<String, Object> oldQuery = this.database.getQuery(this.tableId, queryUUID);

			String times;
			try {
				times = new ObjectMapper().writeValueAsString(status.getCpuTimes());
			} catch (Exception e) {
				log.error("Unable to parse cpu times.", e);
				times = null;
			}

			if (oldQuery == null) {
				SimpleQuery query = status.getQuery().getSimpleQuery();
				long startTimeLong = status.getQuery().getStartTime();

				// Set the query creation time to now or given start time
				Date startTime = (startTimeLong > 0) ? TimeHelper.getAdjustedTime(startTimeLong).getTime() : TimeHelper.now().getTime();

				this.database.createQuery(status, query, times, startTime, this.tableId);
			} else if (queryHasChanged(status, times, oldQuery)) {
				this.database.updateQuery(status, times, (Integer) oldQuery.get("ID"));
			}
		}
	}

	private static boolean queryHasChanged(BlurQueryStatus blurQueryStatus, String timesJSON, Map<String, Object> oldQueryInfo) {
		return blurQueryStatus.getState().getValue() == 0
				|| !(timesJSON.equals(oldQueryInfo.get("TIMES"))
						&& blurQueryStatus.getCompleteShards() == (Integer) oldQueryInfo.get("COMPLETE_SHARDS") && blurQueryStatus.getState()
						.getValue() == (Integer) oldQueryInfo.get("STATE"));
	}
}