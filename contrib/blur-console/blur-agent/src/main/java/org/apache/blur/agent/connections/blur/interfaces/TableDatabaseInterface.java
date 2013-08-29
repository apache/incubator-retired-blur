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
public interface TableDatabaseInterface {
	int getTableId(int clusterId, String tableName);

	void updateTableSchema(final int tableId, final String schema, final String tableAnalyzer);

	void updateTableServer(final int tableId, final String server);

	void updateTableStats(final int tableId, final Long tableBytes, final Long tableRecordCount, final Long tableRowCount);
}