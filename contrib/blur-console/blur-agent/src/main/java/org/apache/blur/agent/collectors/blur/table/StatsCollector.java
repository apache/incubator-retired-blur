package org.apache.blur.agent.collectors.blur.table;

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
import org.apache.blur.agent.connections.blur.interfaces.TableDatabaseInterface;
import org.apache.blur.agent.exceptions.NullReturnedException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;


public class StatsCollector implements Runnable {
	private static final Log log = LogFactory.getLog(StatsCollector.class);

	private final Iface blurConnection;
	private final String tableName;
	private final int tableId;
	private final TableDatabaseInterface database;

	public StatsCollector(Iface connection, String tableName, int tableId, TableDatabaseInterface database) {
		this.blurConnection = connection;
		this.tableName = tableName;
		this.tableId = tableId;
		this.database = database;
	}

	@Override
	public void run() {
		try {
			TableStats tableStats = blurConnection.getTableStats(this.tableName);

			if (tableStats == null) {
				throw new NullReturnedException("No table statistics were returned!");
			}

			this.database.updateTableStats(tableId, tableStats.getBytes(), tableStats.getQueries(), tableStats.getRecordCount(),
					tableStats.getRowCount());
		} catch (BlurException e) {
			log.error("Unable to get table stats for table [" + tableId + "].", e);
		} catch (DataAccessException e) {
			log.error("An error occurred while writing the server to the database.", e);
		} catch (NullReturnedException e) {
			log.error(e.getMessage(), e);
		} catch (Exception e) {
			log.error("An unknown error occurred in the TableServerCollector.", e);
		}
	}
}