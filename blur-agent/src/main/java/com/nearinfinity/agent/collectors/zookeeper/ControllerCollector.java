package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.agent.connections.zookeeper.interfaces.ControllerDatabaseInterface;

public class ControllerCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ControllerCollector.class);

  private final int zookeeperId;
  private final ZooKeeper zookeeper;
  private final ControllerDatabaseInterface database;

  public ControllerCollector(int zookeeperId, ZooKeeper zookeeper, ControllerDatabaseInterface database) {
    this.zookeeperId = zookeeperId;
    this.zookeeper = zookeeper;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      List<String> onlineControllers = this.zookeeper.getChildren("/blur/online-controller-nodes", false);
      this.database.markOfflineControllers(onlineControllers, this.zookeeperId);
      updateOnlineControllers(onlineControllers);
    } catch (KeeperException e) {
      log.error("Error talking to zookeeper in ControllerCollector.", e);
    } catch (InterruptedException e) {
      log.error("Zookeeper session expired in ControllerCollector.", e);
    }

  }

  private void updateOnlineControllers(List<String> controllers) throws KeeperException, InterruptedException {
    for (String controller : controllers) {
      String blurVersion = "UNKNOWN";

      byte[] b = this.zookeeper.getData("/blur/online-controller-nodes", false, null);
      if (b != null && b.length > 0) {
        blurVersion = new String(b);
      }

      this.database.updateOnlineController(controller, zookeeperId, blurVersion);
    }
  }
}
