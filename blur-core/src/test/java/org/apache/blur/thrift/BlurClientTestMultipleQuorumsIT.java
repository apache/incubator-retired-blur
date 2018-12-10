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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.blur.MiniCluster;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Splitter;

public class BlurClientTestMultipleQuorumsIT {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurClientTest"));

  @BeforeClass
  public static void startup() throws IOException, BlurException, TException {
    SuiteCluster.setupMiniCluster(BlurClientTestMultipleQuorumsIT.class);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SuiteCluster.shutdownMiniCluster(BlurClientTestMultipleQuorumsIT.class);
  }

  @After
  public void teadown() {
    BlurClient.closeZooKeeper();
  }

  @Test
  public void testMultipleQuorums() throws BlurException, TException {
    File testDirectory = new File(TMPDIR, "testMultipleQuorums").getAbsoluteFile();
    testDirectory.mkdirs();

    MiniCluster cluster2 = new MiniCluster();
    cluster2.startBlurCluster(new File(testDirectory, "cluster2").getAbsolutePath(), 2, 1, true, false);

    Iface client1 = BlurClient.getClientFromZooKeeperConnectionStr(SuiteCluster.getZooKeeperConnStr());
    Iface client2 = BlurClient.getClientFromZooKeeperConnectionStr(cluster2.getZkConnectionString());

    List<String> controllerServerList1 = client1.controllerServerList();
    List<String> controllerServerList1FromConnectionStr = getList(SuiteCluster.getControllerConnectionStr());
    List<String> controllerServerList2 = client2.controllerServerList();
    List<String> controllerServerList2FromConnectionStr = getList(cluster2.getControllerConnectionStr());

    Collections.sort(controllerServerList1);
    Collections.sort(controllerServerList1FromConnectionStr);
    Collections.sort(controllerServerList2);
    Collections.sort(controllerServerList2FromConnectionStr);

    cluster2.shutdownBlurCluster();

    assertEquals(controllerServerList1FromConnectionStr, controllerServerList1);
    assertEquals(controllerServerList2FromConnectionStr, controllerServerList2);
    assertFalse(controllerServerList1.equals(controllerServerList2));

  }

  private List<String> getList(String controllerConnectionStr) {
    Splitter splitter = Splitter.on(',');
    List<String> results = new ArrayList<String>();
    for (String s : splitter.split(controllerConnectionStr)) {
      results.add(s);
    }
    return results;
  }
}
