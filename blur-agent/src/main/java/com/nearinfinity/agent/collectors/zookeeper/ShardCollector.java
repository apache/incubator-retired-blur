package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.agent.connections.zookeeper.interfaces.ShardsDatabaseInterface;
import com.nearinfinity.agent.mailer.AgentMailer;

public class ShardCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ShardCollector.class);

  private final int clusterId;
  private final String clusterName;
  private final ZooKeeper zookeeper;
  private final ShardsDatabaseInterface database;

  public ShardCollector(int clusterId, String clusterName, ZooKeeper zookeeper, ShardsDatabaseInterface database) {
    this.clusterId = clusterId;
    this.clusterName = clusterName;
    this.zookeeper = zookeeper;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      List<String> shards = this.zookeeper.getChildren("/blur/clusters/" + clusterName + "/online/shard-nodes", false);
      int recentlyOffline = this.database.markOfflineShards(shards, this.clusterId);
      if (recentlyOffline > 0){
        AgentMailer.getMailer().notifyShardOffline(this.database.getRecentOfflineShardNames(recentlyOffline));
      }
      updateOnlineShards(shards);
    } catch (KeeperException e) {
      log.error("Error talking to zookeeper in ShardCollector.", e);
    } catch (InterruptedException e) {
      log.error("Zookeeper session expired in ShardCollector.", e);
    }
  }

  private void updateOnlineShards(List<String> shards) throws KeeperException, InterruptedException {
    for (String shard : shards) {
      String blurVersion = "UNKNOWN";

      byte[] b = this.zookeeper.getData("/blur/clusters/" + clusterName + "/online/shard-nodes/" + shard, false, null);
      if (b != null && b.length > 0) {
        blurVersion = new String(b);
      }

      this.database.updateOnlineShard(shard, this.clusterId, blurVersion);
    }
  }
}
