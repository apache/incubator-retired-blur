package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class ZookeeperSystemTime {
  public static void main(String[] args) throws InterruptedException, KeeperException, IOException, BlurException {
    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper("localhost");
    long tolerance = 3000;
    checkSystemTime(zooKeeper, tolerance);
  }

  public static void checkSystemTime(ZooKeeper zooKeeper, long tolerance) throws KeeperException, InterruptedException, BlurException {
    String path = zooKeeper.create("/" + UUID.randomUUID().toString(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    long now = System.currentTimeMillis();
    Stat stat = zooKeeper.exists(path, false);
    zooKeeper.delete(path, -1);
    long ctime = stat.getCtime();

    long low = now - tolerance;
    long high = now + tolerance;
    if (!(low <= ctime && ctime <= high)) {
      throw new BlurException("The system time is too far out of sync with Zookeeper, check your system time and try again.", null);
    }
  }
}
