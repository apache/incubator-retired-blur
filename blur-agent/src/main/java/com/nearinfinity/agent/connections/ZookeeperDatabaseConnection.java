package com.nearinfinity.agent.connections;

import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.connections.interfaces.ZookeeperDatabaseInterface;

public class ZookeeperDatabaseConnection implements ZookeeperDatabaseInterface {

  private final JdbcTemplate jdbc;

  public ZookeeperDatabaseConnection(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }
  
  @Override
  public int getZookeeperId(String name){
    return this.jdbc.queryForInt("select id from zookeepers where name = ?", name);
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
  public int insertOrUpdateCluster(boolean safeMode, String cluster, int zookeeperId) {
    int updateCount = this.jdbc.update("update clusters set safe_mode=? where name=? and zookeeper_id=?", safeMode, cluster, zookeeperId);
    if (updateCount == 0) {
      this.jdbc.update("insert into clusters (name, zookeeper_id, safe_mode) values (?, ?, ?)", cluster, zookeeperId, safeMode);
    }
    return this.jdbc.queryForInt("select id from clusters where name=? and zookeeper_id=?", cluster, zookeeperId);
  }
}
