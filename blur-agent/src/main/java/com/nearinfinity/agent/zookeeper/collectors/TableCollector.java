package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class TableCollector {
	private ZooKeeper zk;
	private JdbcTemplate jdbc;
	private String clusterName;
	private int clusterId;
	
	private TableCollector(InstanceManager manager, JdbcTemplate jdbc, int clusterId, String clusterName) {
		this.zk = manager.getInstance();
		this.jdbc = jdbc;
		this.clusterId = clusterId;
		this.clusterName = clusterName;
		
		updateTables();
	}
	
	private void updateTables() {
		List<String> tables = getTables();
		markOfflineTables(tables);
		updateOnlineTables(tables);
	}
	
	private List<String> getTables() {
		try {
			return zk.getChildren("/blur/clusters/" + clusterName + "/tables", false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}
	
	private void markOfflineTables(List<String> tables) {
		if (tables.isEmpty()) {
			jdbc.update("update blur_tables set status = 0 where cluster_id=?", clusterId);
		} else {
			jdbc.update("update blur_tables set status = 0 where cluster_id='" + clusterId + "' and table_name not in ('" + StringUtils.join(tables, "','") + "')");
		}
	}
	
	private void updateOnlineTables(List<String> tables) {
		for (String table : tables) {
			String uri = null;
			boolean enabled = false;
			
			try {
				uri = new String(zk.getData("/blur/clusters/" + clusterName + "/tables/" + table + "/uri", false, null));
				enabled = zk.getChildren("/blur/clusters/" + clusterName + "/tables/" + table, false).contains("enabled");
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			List<Map<String, Object>> instances = jdbc.queryForList("select id from blur_tables where table_name = ? and cluster_id=?", new Object[]{table, clusterId});
			if (instances.isEmpty()) {
				jdbc.update("insert into blur_tables (table_name, table_uri, status, cluster_id) values (?, ?, ?, ?)", new Object[]{table, uri, (enabled ? 2 : 1), clusterId});
			} else {
				jdbc.update("update blur_tables set status=? where table_name=? and cluster_id=?", new Object[]{(enabled ? 2 : 1), table, clusterId});
			}
		}
	}
	
	public static void collect(InstanceManager manager, JdbcTemplate jdbc, int clusterId, String clusterName) {
		new TableCollector(manager, jdbc, clusterId, clusterName);
	}
}
