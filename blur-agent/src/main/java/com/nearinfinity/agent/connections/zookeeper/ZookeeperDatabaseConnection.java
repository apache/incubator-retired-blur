package com.nearinfinity.agent.connections.zookeeper;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.zookeeper.interfaces.ClusterDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ControllerDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ShardsDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ZookeeperDatabaseInterface;
import com.nearinfinity.agent.types.TimeHelper;

public class ZookeeperDatabaseConnection implements ZookeeperDatabaseInterface, ControllerDatabaseInterface, TableDatabaseInterface,
		ShardsDatabaseInterface, ClusterDatabaseInterface {

	private final JdbcTemplate jdbc;

	public ZookeeperDatabaseConnection(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	@Override
	public void setZookeeperOnline(int zookeeperId) {
		this.jdbc.update("update zookeepers set status=? where id=?", 1, zookeeperId);
	}

	@Override
	public void setZookeeperWarning(int zookeeperId) {
		this.jdbc.update("update zookeepers set status=? where id=?", 2, zookeeperId);
	}

	@Override
	public void setZookeeperFailure(int zookeeperId) {
		this.jdbc.update("update zookeepers set status=? where id=?", 3, zookeeperId);
	}

	@Override
	public void setZookeeperOffline(int zookeeperId) {
		this.jdbc.update("update zookeepers set status=? where id=?", 0, zookeeperId);
	}

	@Override
	public int insertOrUpdateZookeeper(String name, String url, String blurConnection) {
		int updatedCount = jdbc.update("update zookeepers set url=? where name=?", url, name);

		if (updatedCount == 0) {
			jdbc.update("insert into zookeepers (name, url, blur_urls) values (?, ?, ?)", name, url, blurConnection);
		}

		return jdbc.queryForInt("select id from zookeepers where name = ?", name);
	}

	@Override
	public int insertOrUpdateCluster(boolean safeMode, String cluster, int zookeeperId) {
		int updateCount = this.jdbc.update("update clusters set safe_mode=? where name=? and zookeeper_id=?", safeMode, cluster, zookeeperId);
		if (updateCount == 0) {
			this.jdbc.update("insert into clusters (name, zookeeper_id, safe_mode) values (?, ?, ?)", cluster, zookeeperId, safeMode);
		}
		return this.jdbc.queryForInt("select id from clusters where name=? and zookeeper_id=?", cluster, zookeeperId);
	}

	@Override
	public int markOfflineControllers(List<String> onlineControllers, int zookeeperId) {
		if (onlineControllers.isEmpty()) {
			return this.jdbc.update("update blur_controllers set status=0, updated_at=? where zookeeper_id = ?", TimeHelper.now().getTime(),
					zookeeperId);
		} else {
			return this.jdbc.update(
					"update blur_controllers set status=0, updated_at=? where status!=0 and node_name not in ('"
							+ StringUtils.join(onlineControllers, "','") + "') and zookeeper_id = ?", TimeHelper.now().getTime(), zookeeperId);
		}
	}

	@Override
	public int markOfflineShards(List<String> onlineShards, int clusterId) {
		if (onlineShards.isEmpty()) {
			return this.jdbc.update("update blur_shards set status=0 updated_at=? where cluster_id = ?", TimeHelper.now().getTime(), clusterId);
		} else {
			return this.jdbc.update(
					"update blur_shards set status=0, updated_at=? where status!=0 and node_name not in ('" + StringUtils.join(onlineShards, "','")
							+ "') and cluster_id=?", TimeHelper.now().getTime(), clusterId);
		}
	}

	@Override
	public void markDeletedTables(List<String> onlineTables, int clusterId) {
		if (onlineTables.isEmpty()) {
			this.jdbc.update("delete from blur_tables where cluster_id=?", clusterId);
		} else {
			this.jdbc.update("delete from blur_tables where cluster_id=? and table_name not in ('"
					+ StringUtils.join(onlineTables, "','") + "')", clusterId);
		}
	}

	@Override
	public void updateOnlineController(String controller, int zookeeperId, String blurVersion) {
		int zookeeperStatus = this.jdbc.queryForInt("select status from zookeepers where id=?", zookeeperId);
		int status = (zookeeperStatus == 0 || zookeeperStatus == 2) ? 2 : 1;
		int updatedCount = this.jdbc.update(
				"update blur_controllers set status=?, blur_version=?, updated_at=? where node_name=? and zookeeper_id =?", status, blurVersion,
				TimeHelper.now().getTime(), controller, zookeeperId);

		if (updatedCount == 0) {
			this.jdbc.update(
					"insert into blur_controllers (node_name, status, zookeeper_id, blur_version, created_at, updated_at) values (?, ?, ?, ?, ?, ?)",
					controller, status, zookeeperId, blurVersion, TimeHelper.now().getTime(), TimeHelper.now().getTime());
		}
	}

	@Override
	public void updateOnlineShard(String shard, int clusterId, String blurVersion) {
		int zookeeperStatus = this.jdbc.queryForInt(
				"select zookeepers.status from zookeepers, clusters where clusters.id=? and clusters.zookeeper_id=zookeepers.id;",
				clusterId);
		int status = (zookeeperStatus == 0 || zookeeperStatus == 2) ? 2 : 1;
		int updatedCount = this.jdbc.update("update blur_shards set status=?, blur_version=?, updated_at=? where node_name=? and cluster_id=?",
				status, blurVersion, TimeHelper.now().getTime(), shard, clusterId);

		if (updatedCount == 0) {
			this.jdbc.update(
					"insert into blur_shards (node_name, status, cluster_id, blur_version, created_at, updated_at) values (?, ?, ?, ?, ?, ?)", shard,
					status, clusterId, blurVersion, TimeHelper.now().getTime(), TimeHelper.now().getTime());
		}
	}

	@Override
	public void updateOnlineTable(String table, int clusterId, String uri, boolean enabled) {
		try{
			int currentStatus = this.jdbc.queryForInt("select status from blur_tables where table_name=? and cluster_id=?", table, clusterId);
			if (enabled && currentStatus != 3){
				this.jdbc.update("update blur_tables set table_uri=?, status=?, updated_at=? where table_name=? and cluster_id=?",
						uri, 4, TimeHelper.now().getTime(), table, clusterId);
				
			}
			if (!enabled && currentStatus != 5) {
				this.jdbc.update("update blur_tables set table_uri=?, status=?, updated_at=? where table_name=? and cluster_id=?",
						uri, 2, TimeHelper.now().getTime(), table, clusterId);
			}
		} catch(EmptyResultDataAccessException e){
			this.jdbc.update("insert into blur_tables (table_name, table_uri, status, cluster_id, updated_at) values (?, ?, ?, ?, ?)", table,
					uri, (enabled ? 4 : 2), clusterId, TimeHelper.now().getTime());
		}
	}

	@Override
	public void setOnlineEnsembleNodes(String ensembleArray, int zookeeperId) {
		this.jdbc.update("update zookeepers set online_ensemble_nodes=? where id=?", ensembleArray, zookeeperId);
	}

	@Override
	public List<String> getRecentOfflineShardNames(int amount) {
		return this.jdbc.queryForList("select node_name from blur_shards where status=0 order by updated_at limit 0, " + amount, String.class);
	}

	@Override
	public List<String> getRecentOfflineControllerNames(int amount) {
		return this.jdbc.queryForList("select node_name from blur_controllers where status=0 order by updated_at limit 0, " + amount,
				String.class);
	}
}
