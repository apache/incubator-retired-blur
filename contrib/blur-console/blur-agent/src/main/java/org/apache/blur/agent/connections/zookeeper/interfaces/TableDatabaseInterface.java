package org.apache.blur.agent.connections.zookeeper.interfaces;

import java.util.List;

public interface TableDatabaseInterface {

	void markDeletedTables(List<String> onlineTables, int clusterId);

	void updateOnlineTable(String table, int clusterId, String uri, boolean enabled);

}
