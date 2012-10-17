package com.nearinfinity.agent.collectors.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import com.nearinfinity.agent.Agent;
import com.nearinfinity.agent.connections.zookeeper.interfaces.ZookeeperDatabaseInterface;

public class ZookeeperCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ZookeeperCollector.class);

  private ZooKeeper zookeeper;
  private CountDownLatch latch;

  private final String url;
  private final String name;
  private final int id;
  private final ZookeeperDatabaseInterface database;

  public ZookeeperCollector(String url, String name, String blurConnection, ZookeeperDatabaseInterface database) {
    this.url = url;
    this.name = name;
    this.database = database;
    this.id = database.insertOrUpdateZookeeper(name, url, blurConnection);

  }

  @Override
  public void run() {
    while (true) {
      try {
        this.latch = new CountDownLatch(1);
        if (this.zookeeper != null) {
          try {
            this.zookeeper.close();
          } catch (InterruptedException e) {}
        }
        this.zookeeper = new ZooKeeper(this.url, 3000, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            KeeperState state = event.getState();
            if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
              log.warn("Zookeeper [" + name + "] disconnected event.");
              closeZookeeper();
            } else if (state == KeeperState.SyncConnected) {
              latch.countDown();
              log.info("Zookeeper [" + name + "] session established.");
            }
          }
        });
      } catch (IOException e) {
        log.error("A zookeeper [" + this.name + "] connection could not be created, waiting 30 seconds.");
        closeZookeeper();
        // Sleep the thread for 30secs to give the Zookeeper a chance to
        // become available.
        try {
          Thread.sleep(30000);
          continue;
        } catch (InterruptedException ex) {
          log.info("Exiting Zookeeper [" + this.name + "] instance");
          return;
        }
      }

      try {
        if (latch.await(10, TimeUnit.SECONDS)) {
          this.database.setZookeeperOnline(this.id);
          new Thread(new ControllerCollector(this.id, this.zookeeper, this.database)).start();
          new Thread(new ClusterCollector(this.id, this.zookeeper, this.database)).start();
        } else {
          closeZookeeper();
        }
      } catch (InterruptedException e) {
        closeZookeeper();
      }

      try {
        Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
      } catch (InterruptedException e) {
        log.info("Exiting Zookeeper [" + this.name + "] instance");
        return;
      }
    }
  }

  private void closeZookeeper() {
    try {
      if (this.zookeeper != null) {
        this.zookeeper.close();
        log.warn("Closing the zookeeper [" + this.name + "] connection.");
      } else {
        log.warn("The zookeeper [" + this.name + "] is already closed.");
      }
    } catch (InterruptedException e) {
      log.error("An error occurred while trying to close the zookeeper [" + this.name + "] connection.");
    } finally {
      this.zookeeper = null;
      this.database.setZookeeperOffline(this.id);
    }
  }
}
