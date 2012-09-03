package com.nearinfinity.blur.thrift;

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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.blur.MiniCluster;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurClusterTest {

  @BeforeClass
  public static void startCluster() {
    MiniCluster.startBlurCluster("./tmp/cluster", 2, 3);
  }

  @AfterClass
  public static void shutdownCluster() {
    MiniCluster.shutdownBlurCluster();
  }

  @Test
  public void testCreateTable() throws BlurException, TException, IOException {
    Blur.Iface client = getClient();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri(MiniCluster.getFileSystemUri().toString() + "/blur/test");
    client.createTable(tableDescriptor);
    List<String> tableList = client.tableList();
    assertEquals(Arrays.asList("test"), tableList);
  }

  private Iface getClient() {
    return BlurClient.getClient(MiniCluster.getControllerConnectionStr());
  }

  @Test
  public void testLoadTable() throws BlurException, TException, InterruptedException {
    Iface client = getClient();
    int length = 100;
    List<RowMutation> mutations = new ArrayList<RowMutation>();
    for (int i = 0; i < length; i++) {
      String rowId = UUID.randomUUID().toString();
      RecordMutation mutation = BlurUtil.newRecordMutation("test", rowId, BlurUtil.newColumn("test", "value"));
      RowMutation rowMutation = BlurUtil.newRowMutation("test", rowId, mutation);
      rowMutation.setWaitToBeVisible(true);
      mutations.add(rowMutation);
    }
    client.mutateBatch(mutations);
    BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("test.test:value");
    blurQuery.setSimpleQuery(simpleQuery);
    BlurResults results = client.query("test", blurQuery);
    assertEquals(length, results.getTotalResults());
  }
}
