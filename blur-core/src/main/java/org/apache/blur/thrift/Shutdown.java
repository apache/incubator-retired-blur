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
package org.apache.blur.thrift;

import static org.apache.blur.utils.BlurConstants.BLUR_CLUSTER;
import static org.apache.blur.utils.BlurConstants.BLUR_CLUSTER_NAME;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TIMEOUT_DEFAULT;

import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.manager.clusterstatus.ZookeeperPathConstants;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Shutdown {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    BlurConfiguration configuration = new BlurConfiguration();
    String zkConnectionStr = ThriftServer.isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION),
        BLUR_ZOOKEEPER_CONNECTION);
    int sessionTimeout = configuration.getInt(BLUR_ZOOKEEPER_TIMEOUT, BLUR_ZOOKEEPER_TIMEOUT_DEFAULT);
    final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr, sessionTimeout);
    String cluster = configuration.get(BLUR_CLUSTER_NAME, BLUR_CLUSTER);
    String shutdownPath = ZookeeperPathConstants.getShutdownPath(cluster);
    zooKeeper.create(shutdownPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          zooKeeper.close();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }));

    while (true) {
      System.out.println("Waiting for shutdown.");
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

}
