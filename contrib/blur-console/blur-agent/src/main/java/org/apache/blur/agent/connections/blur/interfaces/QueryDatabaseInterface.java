package org.apache.blur.agent.connections.blur.interfaces;

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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.SimpleQuery;

public interface QueryDatabaseInterface {
	Map<String, Object> getQuery(int tableId, long UUID);

	List<Long> getRunningQueries(Long tableId);

	void createQuery(BlurQueryStatus status, SimpleQuery query, String times, Date startTime, int tableId);

	void updateQuery(BlurQueryStatus status, String times, int queryId);
	
	void markOrphanedRunningQueriesComplete(Collection<Long> queries);
}
