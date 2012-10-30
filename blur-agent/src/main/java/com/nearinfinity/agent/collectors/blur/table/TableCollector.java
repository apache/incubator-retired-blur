package com.nearinfinity.agent.collectors.blur.table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nearinfinity.agent.connections.blur.interfaces.TableDatabaseInterface;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

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
