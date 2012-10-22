package com.nearinfinity.agent.connections.zookeeper;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.zookeeper.interfaces.ClusterDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ControllerDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ShardsDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ZookeeperDatabaseInterface;

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
  public void markOfflineControllers(List<String> onlineControllers, int zookeeperId) {
    if (onlineControllers.isEmpty()) {
      this.jdbc.update("update blur_controllers set status = 0 where zookeeper_id = ?", zookeeperId);
    } else {
      this.jdbc.update("update blur_controllers set status = 0 where node_name not in ('" + StringUtils.join(onlineControllers, "','")
          + "') and zookeeper_id = ?", zookeeperId);
    }
  }

  @Override
  public void markOfflineShards(List<String> onlineShards, int clusterId) {
    if (onlineShards.isEmpty()) {
      this.jdbc.update("update blur_shards set status = 0 where cluster_id = ?", clusterId);
    } else {
      this.jdbc.update("update blur_shards set status = 0 where node_name not in ('" + StringUtils.join(onlineShards, "','")
          + "') and cluster_id=?", clusterId);
    }
  }

  @Override
  public void markOfflineTables(List<String> onlineTables, int clusterId) {
    if (onlineTables.isEmpty()) {
      this.jdbc.update("update blur_tables set status = 0 where cluster_id=?", clusterId);
    } else {
      this.jdbc.update(
          "update blur_tables set status = 0 where cluster_id=? and table_name not in ('" + StringUtils.join(onlineTables, "','") + "')",
          clusterId);
    }
  }

  @Override
  public void updateOnlineController(String controller, int zookeeperId, String blurVersion) {
    int updatedCount = this.jdbc.update("update blur_controllers set status=1, blur_version=? where node_name=? and zookeeper_id =?",
        blurVersion, controller, zookeeperId);

    if (updatedCount == 0) {
      this.jdbc.update("insert into blur_controllers (node_name, status, zookeeper_id, blur_version) values (?, 1, ?, ?)", controller,
          zookeeperId, blurVersion);
    }
  }

  @Override
  public void updateOnlineShard(String shard, int clusterId, String blurVersion) {
    int updatedCount = this.jdbc.update("update blur_shards set status=1, blur_version=? where node_name=? and cluster_id=?", blurVersion,
        shard, clusterId);

    if (updatedCount == 0) {
      this.jdbc.update("insert into blur_shards (node_name, status, cluster_id, blur_version) values (?, 1, ?, ?)", shard, clusterId,
          blurVersion);
    }
  }

  @Override
  public void updateOnlineTable(String table, int clusterId, String uri, boolean enabled) {
    int updatedCount = this.jdbc.update("update blur_tables set table_uri=?, status=? where table_name=? and cluster_id=?", uri,
        (enabled ? 4 : 2), table, clusterId);

    if (updatedCount == 0) {
      this.jdbc.update("insert into blur_tables (table_name, table_uri, status, cluster_id) values (?, ?, ?, ?)", table, uri, (enabled ? 4
          : 2), clusterId);
    }
  }
}
