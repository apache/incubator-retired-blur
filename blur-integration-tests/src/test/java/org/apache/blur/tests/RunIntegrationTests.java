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
package org.apache.blur.tests;

import java.io.IOException;
import java.util.List;

import org.apache.blur.analysis.ThriftFieldManagerTestIT;
import org.apache.blur.command.ReconnectWhileCommandIsRunningIntTestsIT;
import org.apache.blur.hive.BlurSerDeTestIT;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatusTestIT;
import org.apache.blur.manager.indexserver.MasterBasedDistributedLayoutFactoryTestIT;
import org.apache.blur.manager.indexserver.SafeModeTestIT;
import org.apache.blur.manager.writer.SharedMergeSchedulerThroughputTestIT;
import org.apache.blur.mapreduce.lib.BlurInputFormatTestIT;
import org.apache.blur.mapreduce.lib.BlurOutputFormatMiniClusterTestIT;
import org.apache.blur.mapreduce.lib.BlurOutputFormatTestIT;
import org.apache.blur.mapreduce.lib.update.DriverTestIT;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClientTestMultipleQuorumsIT;
import org.apache.blur.thrift.BlurClusterTestNoSecurityIT;
import org.apache.blur.thrift.BlurClusterTestSecurityIT;
import org.apache.blur.thrift.FacetTestsIT;
import org.apache.blur.thrift.SuiteCluster;
import org.apache.blur.thrift.TermsTestsIT;
import org.apache.blur.thrift.ThriftServerTestIT;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ ThriftFieldManagerTestIT.class, ReconnectWhileCommandIsRunningIntTestsIT.class,
    ZookeeperClusterStatusTestIT.class, MasterBasedDistributedLayoutFactoryTestIT.class, SafeModeTestIT.class,
    SharedMergeSchedulerThroughputTestIT.class, BlurClientTestMultipleQuorumsIT.class,
    BlurClusterTestNoSecurityIT.class, BlurClusterTestSecurityIT.class, FacetTestsIT.class, TermsTestsIT.class,
    ThriftServerTestIT.class, BlurSerDeTestIT.class, DriverTestIT.class, BlurInputFormatTestIT.class,
    BlurOutputFormatMiniClusterTestIT.class, BlurOutputFormatTestIT.class })
public class RunIntegrationTests {

  @BeforeClass
  public static void startup() throws IOException, BlurException, TException {
    SuiteCluster.setupMiniCluster(RunIntegrationTests.class);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SuiteCluster.shutdownMiniCluster(RunIntegrationTests.class);
  }
  
  @After
  public void tearDown() throws BlurException, TException, IOException {
    Iface client = SuiteCluster.getClient();
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      client.disableTable(table);
      client.removeTable(table, true);
    }
  }

}
