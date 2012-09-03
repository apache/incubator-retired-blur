package org.apache.blur.thrift;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.UUID;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


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
