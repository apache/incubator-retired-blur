package org.apache.blur.zookeeper;

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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkUtilsTest {

  private static final int ANY_VERSION = -1;
  private static ZkMiniCluster _zkMiniCluster;

  @BeforeClass
  public static void setupZookeeper() {
    _zkMiniCluster = new ZkMiniCluster();
    _zkMiniCluster.startZooKeeper(new File("target/ZkUtilsTest").getAbsolutePath(), true);
  }

  @AfterClass
  public static void tearDownZookeeper() {
    _zkMiniCluster.shutdownZooKeeper();
  }

  private ZooKeeper _zooKeeper;

  @Before
  public void setUp() throws IOException, InterruptedException {
    final Object lock = new Object();
    synchronized (lock) {
      _zooKeeper = new ZooKeeperClient(_zkMiniCluster.getZkConnectionString(), 10000, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          synchronized (lock) {
            lock.notifyAll();
          }
        }
      });
      lock.wait();
    }

  }

  @After
  public void tearDown() throws Exception {
    delete("/test/foo/bar");
    delete("/test/foo");
    delete("/test");
    _zooKeeper.close();
  }

  @Test
  public void testMkNodesStrWhenNoNodesExist() throws Exception {
    assertDoesNotExist("/test/foo/bar");
    ZkUtils.mkNodesStr(_zooKeeper, "/test/foo/bar");
    assertExists("/test/foo/bar");
  }

  @Test
  public void testMkNodesStrWhenSomeNodesExist() throws Exception {
    create("/test");
    create("/test/foo");
    assertThat(_zooKeeper.exists("/test/foo/bar", false), is(nullValue()));
    ZkUtils.mkNodesStr(_zooKeeper, "/test/foo/bar");
    assertExists("/test/foo/bar");
  }

  @Test
  public void testMkNodesWhenNoNodesExist() throws Exception {
    assertDoesNotExist("/test/foo/bar");
    ZkUtils.mkNodes(_zooKeeper, "test", "foo", "bar");
    assertExists("/test/foo/bar");
  }

  @Test
  public void testMkNodesWhenSomeNodesExist() throws Exception {
    create("/test");
    create("/test/foo");
    assertThat(_zooKeeper.exists("/test/foo/bar", false), is(nullValue()));
    ZkUtils.mkNodes(_zooKeeper, "/test", "foo", "bar");
    assertExists("/test/foo/bar");
  }

  private void create(String path) throws KeeperException, InterruptedException {
    _zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  private void delete(String path) throws KeeperException, InterruptedException {
    if (_zooKeeper.exists(path, false) != null) {
      _zooKeeper.delete(path, ANY_VERSION);
    }
  }

  private void assertDoesNotExist(String path) throws InterruptedException, KeeperException {
    assertThat(_zooKeeper.exists(path, false), is(nullValue()));
  }

  private void assertExists(String path) throws InterruptedException, KeeperException {
    assertThat(_zooKeeper.exists(path, false), is(notNullValue()));
  }

}
