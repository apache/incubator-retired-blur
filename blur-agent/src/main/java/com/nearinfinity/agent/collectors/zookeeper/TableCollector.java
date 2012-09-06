package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.nearinfinity.agent.collectors.Collector;
import com.nearinfinity.agent.types.InstanceManager;

public class TableCollector extends Collector {
	private String clusterName;
	private int clusterId;

//	private static final Log log = LogFactory.getLog(TableCollector.class);

	private TableCollector(InstanceManager manager, int clusterId, String clusterName) throws KeeperException,
			InterruptedException {
		super(manager);
		this.clusterId = clusterId;
		this.clusterName = clusterName;

		updateTables();
	}

	private void updateTables() throws KeeperException, InterruptedException {
		List<String> tables = getTables();
		markOfflineTables(tables);
		updateOnlineTables(tables);
	}

	private List<String> getTables() throws KeeperException, InterruptedException {
		return getZk().getChildren("/blur/clusters/" + clusterName + "/tables", false);
	}

	private void markOfflineTables(List<String> tables) {
		if (tables.isEmpty()) {
			getJdbc().update("update blur_tables set status = 0 where cluster_id=?", clusterId);
		} else {
			getJdbc().update(
					"update blur_tables set status = 0 where cluster_id=? and table_name not in ('"
							+ StringUtils.join(tables, "','") + "')", clusterId);
		}
	}

	private void updateOnlineTables(List<String> tables) throws KeeperException, InterruptedException {
		for (String table : tables) {
			String tablePath = "/blur/clusters/" + clusterName + "/tables/" + table;

			String uri = new String(getZk().getData(tablePath + "/uri", false, null));
			boolean enabled = getZk().getChildren(tablePath, false).contains("enabled");

			int updatedCount = getJdbc().update(
					"update blur_tables set table_uri=?, status=? where table_name=? and cluster_id=?", uri,
					(enabled ? 4 : 2), table, clusterId);

			if (updatedCount == 0) {
				getJdbc().update(
						"insert into blur_tables (table_name, table_uri, status, cluster_id) values (?, ?, ?, ?)",
						table, uri, (enabled ? 4 : 2), clusterId);
			}
		}
	}

	public static void collect(InstanceManager manager, int clusterId, String clusterName) throws KeeperException,
			InterruptedException {
		new TableCollector(manager, clusterId, clusterName);
	}
}
