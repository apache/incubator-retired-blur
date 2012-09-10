package com.nearinfinity.agent.types;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

public class ZookeeperConnection {
  private String name;
  private String url;
  private int instanceId;
  private JdbcTemplate jdbc;
  private ZooKeeper zookeeper;
  private Properties props;

  public ZookeeperConnection(String name, String url, JdbcTemplate jdbc, Properties props) {
    this.name = name;
    this.url = url;
    this.jdbc = jdbc;
    this.props = props;
    
    initializeZkInstanceModel();
    updateZookeeperStatus(false);
  }
  
  public ZooKeeper getZookeeper() {
    return zookeeper;
  }
  
  public void setZookeeper(ZooKeeper zookeeper){
    this.zookeeper = zookeeper;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public JdbcTemplate getJdbc() {
    return jdbc;
  }

  public String getName() {
    return name;
  }
  
  public void updateZookeeperStatus(boolean online) {
    jdbc.update("update zookeepers set status=? where id=?", (online ? 1 : 0), instanceId);
  }
  
  public void closeZookeeper() {
    if (zookeeper != null) {
      try {
        zookeeper.close();
      } catch (InterruptedException e1) {}
    }
    zookeeper = null;
  }
  
  private void initializeZkInstanceModel() {
    String blurConnection = props.getProperty("blur." + name + ".url");

    int updatedCount = jdbc.update("update zookeepers set url=?, blur_urls=? where name=?", url, blurConnection,
        name);

    if (updatedCount == 0) {
      jdbc.update("insert into zookeepers (name, url, blur_urls) values (?, ?, ?)", name, url, blurConnection);
    }

    instanceId = jdbc.queryForInt("select id from zookeepers where name = ?", new Object[] { name });
  }
}
