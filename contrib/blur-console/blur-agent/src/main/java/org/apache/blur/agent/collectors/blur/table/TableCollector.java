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
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TableCollector implements Runnable {
	private static final Log log = LogFactory.getLog(TableCollector.class);

	private final Iface blurConnection;
	private final String tableName;
	private final int tableId;
	private final TableDatabaseInterface database;

	public TableCollector(Iface connection, String tableName, int tableId, TableDatabaseInterface database) {
		this.blurConnection = connection;
		this.tableName = tableName;
		this.database = database;
		this.tableId = tableId;
	}

	@Override
	public void run() {
		try {
			TableDescriptor descriptor;
			try {
				descriptor = blurConnection.describe(tableName);
			} catch (Exception e) {
				log.error("An error occured while trying to describe the table [" + tableName + "], skipping table", e);
				return;
			}

			/* spawn the different table info collectors */
			if (descriptor.isEnabled) {
				new Thread(new SchemaCollector(this.blurConnection, this.tableName, this.tableId, descriptor, this.database),
						"Table Schema Collector - " + this.tableName).start();
			}
			new Thread(new ServerCollector(this.blurConnection, this.tableName, this.tableId, this.database), "Table Server Collector - "
					+ this.tableName).start();
			new Thread(new StatsCollector(this.blurConnection, this.tableName, this.tableId, this.database), "Table Stats Collector - "
					+ this.tableName).start();

		} catch (Exception e) {
			log.error("An unknown error occurred.", e);
		}
	}
}
