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
package org.apache.blur.analysis;

import java.io.IOException;

import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.SuiteCluster;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ThriftFieldManagerTestIT extends BaseFieldManagerTest {

  private static TableDescriptor _tableDescriptor;

  @BeforeClass
  public static void startup() throws IOException, BlurException, TException {
    SuiteCluster.setupMiniCluster(ThriftFieldManagerTestIT.class);
  }

  @AfterClass
  public static void shutdown() throws IOException {
    SuiteCluster.shutdownMiniCluster(ThriftFieldManagerTestIT.class);
  }

  public Iface getClient() throws IOException {
    return SuiteCluster.getClient();
  }

  @Before
  public void setup() throws BlurException, TException, IOException {
    _tableDescriptor = new TableDescriptor();
    _tableDescriptor.setName("ThriftFieldManagerTest");
    _tableDescriptor.setShardCount(1);
    String fileSystemUri = SuiteCluster.getFileSystemUri();
    _tableDescriptor.setTableUri(fileSystemUri + "/tables/ThriftFieldManagerTest");
    Iface client = getClient();
    client.createTable(_tableDescriptor);
  }

  @After
  public void teardown() throws BlurException, TException, IOException {
    Iface client = getClient();
    String table = _tableDescriptor.getName();
    client.disableTable(table);
    client.removeTable(table, true);
  }

  @Override
  protected FieldManager newFieldManager(boolean create) throws IOException {
    TableContext tableContext = TableContext.create(_tableDescriptor, true, getClient());
    return tableContext.getFieldManager();
  }

}
