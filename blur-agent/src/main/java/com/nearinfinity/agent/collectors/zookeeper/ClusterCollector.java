package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.agent.connections.zookeeper.interfaces.ClusterDatabaseInterface;

public class ClusterCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ClusterCollector.class);

  private final int zookeeperId;
  private final ZooKeeper zookeeper;
  private final ClusterDatabaseInterface database;

  public ClusterCollector(int zookeeperId, ZooKeeper zookeeper, ClusterDatabaseInterface database) {
    this.zookeeperId = zookeeperId;
    this.zookeeper = zookeeper;
    this.database = database;
  }

  @Override
  public void run() {
    List<String> onlineClusters;
    try {
      onlineClusters = zookeeper.getChildren("/blur/clusters", false);
    } catch (Exception e) {
      log.error("Error getting clusters from zookeeper in ClusterCollector.", e);
      return;
    }
    
    for (String cluster : onlineClusters) {
      try {
        boolean safeMode = isClusterInSafeMode(cluster);
        int clusterId = this.database.insertOrUpdateCluster(safeMode, cluster, zookeeperId);
  
        new Thread(new ShardCollector(clusterId, cluster, this.zookeeper, this.database), "Shard Collector - " + cluster).start();
        new Thread(new TableCollector(clusterId, cluster, this.zookeeper, this.database), "Table Collector - " + cluster).start();
      } catch (KeeperException e) {
        log.error("Error talking to zookeeper in ClusterCollector.", e);
      } catch (InterruptedException e) {
        log.error("Zookeeper session expired in ClusterCollector.", e);
      }
    }
    
  }

  private boolean isClusterInSafeMode(String cluster) throws KeeperException, InterruptedException {
    String blurSafemodePath = "/blur/clusters/" + cluster + "/safemode";
    Stat stat = this.zookeeper.exists(blurSafemodePath, false);
    if (stat == null) {
      return false;
    }

    byte[] data = this.zookeeper.getData(blurSafemodePath, false, stat);
    if (data == null) {
      return false;
    }

    long timestamp = Long.parseLong(new String(data));
    long waitTime = timestamp - System.currentTimeMillis();
    if (waitTime > 0) {
      return true;
    }
    return false;
  }
}
