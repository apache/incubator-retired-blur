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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZookeeperPathConstants;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ThriftServerTestIT extends BaseClusterTest {

  @Test
  public void testRootedZkPath() throws IOException, KeeperException, InterruptedException {
    ZooKeeper zk = null;
    ZooKeeper nonRooted = null;
    try {
      BlurConfiguration conf = new BlurConfiguration();
      conf.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, getZkConnString() + "/rooted");
      zk = ThriftServer.setupZookeeper(conf, "default");

      assertNotNull("Should be addressable via rooted path",
          zk.exists(ZookeeperPathConstants.getClusterPath("default"), false));

      nonRooted = ZkUtils.newZooKeeper(getZkConnString(), 10000);
      assertTrue("Should really be rooted.",
          ZkUtils.exists(nonRooted, "/rooted" + ZookeeperPathConstants.getClusterPath("default")));
    } finally {
      if (zk != null) {
        zk.close();
      }
      if(nonRooted != null) {
        nonRooted.close();
      }
    }
  }
  @Test
  public void testNonRootedZkPathWithSlash() throws IOException, KeeperException, InterruptedException {
    ZooKeeper zk = null;
    ZooKeeper nonRooted = null;
    try {
      BlurConfiguration conf = new BlurConfiguration();
      conf.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, getZkConnString() + "/");
      zk = ThriftServer.setupZookeeper(conf, "default");

      nonRooted = ZkUtils.newZooKeeper(getZkConnString(), 10000);
      assertTrue("Should really be rooted.",
          ZkUtils.exists(nonRooted,  ZookeeperPathConstants.getClusterPath("default")));
    } finally {
      if (zk != null) {
        zk.close();
      }
      if(nonRooted != null) {
        nonRooted.close();
      }
    }
  }
}
