package com.nearinfinity.agent.collectors.zookeeper;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ZookeeperDatabaseInterface;
import com.nearinfinity.agent.mailer.AgentMailerInterface;

public class ZookeeperCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ZookeeperCollector.class);

  private ZooKeeper zookeeper;
  private boolean connected;

  private final String url;
  private final String name;
  private final int id;
  private final ZookeeperDatabaseInterface database;
  private final AgentMailerInterface mailer;

  public ZookeeperCollector(String url, String name, String blurConnection, ZookeeperDatabaseInterface database, AgentMailerInterface mailer) {
    this.url = url;
    this.name = name;
    this.database = database;
    this.id = database.insertOrUpdateZookeeper(name, url, blurConnection);
    this.mailer = mailer;
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (!this.connected) {
          this.zookeeper = new ZooKeeper(this.url, 3000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
              KeeperState state = event.getState();
              if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
                log.warn("Zookeeper [" + name + "] disconnected event.");
                database.setZookeeperOffline(id);
                mailer.notifyZookeeperOffline(name);
                connected = false;
              } else if (state == KeeperState.SyncConnected) {
                log.info("Zookeeper [" + name + "] session established.");
                database.setZookeeperOnline(id);
                connected = true;
              }
            }
          });
        }
      } catch (IOException e) {
        log.error("A zookeeper [" + this.name + "] connection could not be created, waiting 30 seconds.");
        // Sleep the thread for 30secs to give the Zookeeper a chance to become
        // available.
        try {
          Thread.sleep(30000);
          continue;
        } catch (InterruptedException ex) {
          log.info("Exiting Zookeeper [" + this.name + "] instance");
          return;
        }
      }

      if (this.connected) {
        new Thread(new ControllerCollector(this.id, this.zookeeper, this.database), "Controller Collector - " + this.name).start();
        new Thread(new ClusterCollector(this.id, this.zookeeper, this.database), "Cluster Collector - " + this.name).start();
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        log.info("Exiting Zookeeper [" + this.name + "] instance");
        return;
      }
    }
  }
}
