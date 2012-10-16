package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.agent.connections.interfaces.ZookeeperDatabaseInterface;

public class TableCollector implements Runnable {
  private static final Log log = LogFactory.getLog(TableCollector.class);

  private final int clusterId;
  private final String clusterName;
  private final ZooKeeper zookeeper;
  private final ZookeeperDatabaseInterface database;

  public TableCollector(int clusterId, String clusterName, ZooKeeper zookeeper, ZookeeperDatabaseInterface database) {
    this.clusterId = clusterId;
    this.clusterName = clusterName;
    this.zookeeper = zookeeper;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      List<String> tables = this.zookeeper.getChildren("/blur/clusters/" + clusterName + "/tables", false);
      this.database.markOfflineTables(tables, this.clusterId);
      updateOnlineTables(tables);
    } catch (KeeperException e) {
      log.error("Error talking to zookeeper in TableCollector.", e);
    } catch (InterruptedException e) {
      log.error("Zookeeper session expired in TableCollector.", e);
    }
  }

  private void updateOnlineTables(List<String> tables) throws KeeperException, InterruptedException {
    for (String table : tables) {
      String tablePath = "/blur/clusters/" + clusterName + "/tables/" + table;

      String uri = new String(this.zookeeper.getData(tablePath + "/uri", false, null));
      boolean enabled = this.zookeeper.getChildren(tablePath, false).contains("enabled");

      this.database.updateOnlineTable(table, this.clusterId, uri, enabled);
    }
  }
}
