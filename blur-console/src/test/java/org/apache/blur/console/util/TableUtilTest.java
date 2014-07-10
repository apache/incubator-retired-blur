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
package org.apache.blur.console.util;

import org.apache.blur.console.ConsoleTestBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TableUtilTest extends ConsoleTestBase {

  @Before
  public void ensureCleanTables() throws BlurException, TException, IOException {
    setupConfigIfNeeded();

    Iface client = BlurClient.getClient(Config.getConnectionString());
    List<String> tableList = client.tableList();
    if (!tableList.isEmpty()) {
      for (String table : tableList) {
        client.disableTable(table);
        client.removeTable(table, true);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testGetTableSummariesNoTables() throws BlurException, IOException, TException {
    Map<String, List> data = TableUtil.getTableSummaries();

    assertEquals(0, data.get("tables").size());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testGetTableSummaries() throws BlurException, TException, IOException {
    Iface client = BlurClient.getClient(Config.getConnectionString());

    TableDescriptor td = new TableDescriptor();
    td.setShardCount(11);
    td.setTableUri("file://" + TABLE_PATH + "/tableUnitTable");
    td.setCluster("default");
    td.setName("tableUnitTable");
    td.setEnabled(true);
    client.createTable(td);

    Map<String, List> data = TableUtil.getTableSummaries();

    assertEquals(1, data.get("tables").size());
    assertEquals(0l, ((Map<String, Object>) data.get("tables").get(0)).get("rows"));
    assertEquals(0l, ((Map<String, Object>) data.get("tables").get(0)).get("records"));
    assertEquals("default", ((Map<String, Object>) data.get("tables").get(0)).get("cluster"));
    assertEquals("tableUnitTable", ((Map<String, Object>) data.get("tables").get(0)).get("name"));
    assertEquals(0, ((List<String>) ((Map<String, Object>) data.get("tables").get(0)).get("families")).size());
  }
}
