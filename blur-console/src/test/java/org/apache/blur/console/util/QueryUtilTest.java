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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.blur.console.ConsoleTestBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.junit.Before;
import org.junit.Test;

public class QueryUtilTest extends ConsoleTestBase {
  @Before
  public void setup() throws Exception {
    setupConfigIfNeeded();

    Iface client = Config.getClient();

    if (!client.tableList().contains("queryUnitTable")) {
      TableDescriptor td = new TableDescriptor();
      td.setShardCount(11);
      td.setTableUri("file://" + TABLE_PATH + "/queryUnitTable");
      td.setCluster("default");
      td.setName("queryUnitTable");
      td.setEnabled(true);
      client.createTable(td);

      Record record = new Record("abcd", "fam0", Arrays.asList(new Column[]{new Column("col0", "testvalue")}));
      RecordMutation recordMutation = new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record);
      RowMutation rowMutation = new RowMutation("queryUnitTable", "12345", RowMutationType.REPLACE_ROW, Arrays.asList(new RecordMutation[]{recordMutation}));
      client.mutate(rowMutation);
    }
  }

  @Test
  public void testGetCurrentQueryCount() throws BlurException, IOException, TException {
    UserContext.setUser(new User("testUser",null));
    Iface client = Config.getClient();
    BlurQuery query = new BlurQuery(
        new Query("fam0.col0:*", true, ScoreType.SUPER, null, null),
        null,
        null, //new Selector(false, null, null, null, null, null, 0, 10, null),
        false, 0, 10, 1, 2000, UUID.randomUUID().toString(), "testUser", false, System.currentTimeMillis(), null, null);
    int currentCount = QueryUtil.getCurrentQueryCount();
    client.query("queryUnitTable", query);
    assertEquals(currentCount + 1, QueryUtil.getCurrentQueryCount());
    UserContext.reset();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetQueries() throws IOException, BlurException, TException {
    Iface client = Config.getClient();
    UserContext.setUser(new User("testUser",null));
    BlurQuery query = new BlurQuery(
        new Query("fam0.col0:*", true, ScoreType.SUPER, null, null),
        null,
        null, //new Selector(false, null, null, null, null, null, 0, 10, null),
        false, 0, 10, 1, 2000, UUID.randomUUID().toString(), "testUser", false, System.currentTimeMillis(), null, null);
    int currentCount = 0;
    {
      Map<String, Object> queries = QueryUtil.getQueries();
      Object o = queries.get("queries");
      if (o != null) {
        currentCount = ((List<Map<String, Object>>) o).size();
      }
    }
    client.query("queryUnitTable", query);
    
    UserContext.reset();

    Map<String, Object> queries = QueryUtil.getQueries();

    assertEquals(0, queries.get("slowQueries"));
    assertEquals(currentCount + 1, ((List<Map<String, Object>>) queries.get("queries")).size());
    assertEquals("testUser", ((List<Map<String, Object>>) queries.get("queries")).get(0).get("user"));
  }
}
