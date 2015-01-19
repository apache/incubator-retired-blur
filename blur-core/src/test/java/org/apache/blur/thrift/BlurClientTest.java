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

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.List;

import org.apache.blur.MiniCluster;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.Test;

public class BlurClientTest {
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_BlurClientTest"));

  @Test
  public void testMultipleQuorums() throws BlurException, TException {
    File testDirectory = new File(TMPDIR, "testMultipleQuorums").getAbsoluteFile();
    testDirectory.mkdirs();
    MiniCluster cluster1 = new MiniCluster();
    cluster1.startBlurCluster(new File(testDirectory, "cluster1").getAbsolutePath(), 1, 1, true, false);

    MiniCluster cluster2 = new MiniCluster();
    cluster2.startBlurCluster(new File(testDirectory, "cluster2").getAbsolutePath(), 1, 1, true, false);

    Iface client1 = BlurClient.getClientFromZooKeeperConnectionStr(cluster1.getZkConnectionString());
    Iface client2 = BlurClient.getClientFromZooKeeperConnectionStr(cluster2.getZkConnectionString());

    List<String> controllerServerList1 = client1.controllerServerList();
    List<String> controllerServerList2 = client2.controllerServerList();

    cluster1.shutdownBlurCluster();
    cluster2.shutdownBlurCluster();

    assertFalse(controllerServerList1.equals(controllerServerList2));

  }
}
