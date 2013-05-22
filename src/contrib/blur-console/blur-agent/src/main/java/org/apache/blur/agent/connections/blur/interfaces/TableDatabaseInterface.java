package org.apache.blur.agent.connections.blur.interfaces;

public interface TableDatabaseInterface {
	int getTableId(int clusterId, String tableName);

	void updateTableSchema(final int tableId, final String schema, final String tableAnalyzer);

	void updateTableServer(final int tableId, final String server);

	void updateTableStats(final int tableId, final Long tableBytes, final Long tableQueries, final Long tableRecordCount,
			final Long tableRowCount);
}