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
package org.apache.blur.trace;

import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TRACE_PATH;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.zookeeper.ZkMiniCluster;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZooKeeperTraceStorageTest {

  private static ZkMiniCluster _zkMiniCluster;

  @BeforeClass
  public static void setupZookeeper() {
    _zkMiniCluster = new ZkMiniCluster();
    _zkMiniCluster.startZooKeeper(new File("target/ZooKeeperTraceStorageTest").getAbsolutePath(), true);
  }

  @AfterClass
  public static void tearDownZookeeper() {
    _zkMiniCluster.shutdownZooKeeper();
  }

  private ZooKeeper _zooKeeper;
  private ZooKeeperTraceStorage _storage;

  @Before
  public void setUp() throws IOException, InterruptedException {
    final Object lock = new Object();
    synchronized (lock) {
      _zooKeeper = new ZooKeeper(_zkMiniCluster.getZkConnectionString(), 10000, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          synchronized (lock) {
            lock.notifyAll();
          }
        }
      });
      lock.wait();
    }

    BlurConfiguration configuration = new BlurConfiguration();
    configuration.set(BLUR_ZOOKEEPER_CONNECTION, _zkMiniCluster.getZkConnectionString());
    configuration.set(BLUR_ZOOKEEPER_TRACE_PATH, "/test");
    _storage = new ZooKeeperTraceStorage(configuration);
  }

  @After
  public void tearDown() throws Exception {
    _storage.close();
    ZkUtils.rmr(_zooKeeper, "/test");
    _zooKeeper.close();
  }

  @Test
  public void testStorage() throws IOException {
    Random random = new Random();
    createTraceData(random);
    createTraceData(random);
    createTraceData(random);
    List<String> traceIds = _storage.getTraceIds();
    assertEquals(3, traceIds.size());

    for (String traceId : traceIds) {
      List<String> requestIds = _storage.getRequestIds(traceId);
      assertEquals(4, requestIds.size());
      for (String requestId : requestIds) {
        String contents = _storage.getRequestContentsJson(traceId, requestId);
        assertEquals("{" + requestId + "}", contents);
      }
    }
    
    _storage.removeTrace(traceIds.get(0));
    assertEquals(2, _storage.getTraceIds().size());
  }

  private void createTraceData(Random random) {
    long traceId = Math.abs(random.nextLong());
    String storePath = "/test/" + traceId;
    _storage.storeJson(storePath, Long.toString(traceId), "{" + traceId + "}");
    writeRequest(random, storePath);
    writeRequest(random, storePath);
    writeRequest(random, storePath);
  }

  private void writeRequest(Random random, String storePath) {
    String requestId = Long.toString(random.nextLong());
    _storage.storeJson(storePath, requestId, "{" + requestId + "}");
  }

}
