/**
ing ok * Licensed to the Apache Software Foundation (ASF) under one or more
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.util.BlurThriftHelper;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.junit.Test;

public class BlurClusterTestSecurity extends BlurClusterTestBase {

  private static final String ACL_DISCOVER = "acl-discover";
  private static final String TEST = "test";
  private static final String ACL_READ = "acl-read";
  private static final String DISCOVER = "discover";
  private static final String READ = "read";

  @Override
  protected void setupTableProperties(TableDescriptor tableDescriptor) {
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_RECORD_SECURITY, "true");
  }

  @Override
  protected RowMutation mutate(RowMutation rowMutation) {
    List<RecordMutation> mutations = rowMutation.getRecordMutations();
    for (RecordMutation mutation : mutations) {
      Record record = mutation.getRecord();
      record.addToColumns(new Column(ACL_READ, READ));
      record.addToColumns(new Column(ACL_DISCOVER, DISCOVER));
    }
    return rowMutation;
  }

  @Override
  protected void postTableCreate(TableDescriptor tableDescriptor, Iface client) {
    String name = tableDescriptor.getName();
    try {
      client.addColumnDefinition(name, new ColumnDefinition(TEST, ACL_READ, null, false, ACL_READ, null, false));
      client
          .addColumnDefinition(name, new ColumnDefinition(TEST, ACL_DISCOVER, null, false, ACL_DISCOVER, null, false));
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected User getUser() {
    return new User("testuser", getUserAttributes());
  }

  @Override
  protected Map<String, String> getUserAttributes() {
    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put(BlurConstants.ACL_READ, READ);
    attributes.put(BlurConstants.ACL_DISCOVER, DISCOVER);
    return attributes;
  }

  @Test
  public void testSecurityMaskReadRowQuery() throws BlurException, TException, IOException {
    String tableName = "testSecurity2";
    createTable(tableName);
    Iface client = getClient();
    {
      ColumnDefinition columnDefinition = new ColumnDefinition(TEST, "read_mask", null, false, "read-mask", null, false);
      columnDefinition.setMultiValueField(false);
      client.addColumnDefinition(tableName, columnDefinition);
    }
    List<RowMutation> batch = new ArrayList<RowMutation>();
    for (int i = 0; i < 50; i++) {
      List<RecordMutation> recordMutations = getRecordMutations("a");
      RowMutation mutation1 = new RowMutation(tableName, getRowId(), RowMutationType.REPLACE_ROW, recordMutations);
      addReadMaskColumn(mutation1);
      batch.add(mutation1);
    }
    client.mutateBatch(batch);

    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put(BlurConstants.ACL_READ, "a");
    User user = new User("testuser", attributes);
    UserContext.setUser(user);

    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setSelector(new Selector());
    Query query = new Query();
    query.setRowQuery(true);
    query.setQuery("*");
    blurQuery.setQuery(query);
    blurQuery.setFetch(100);

    BlurResults results = client.query(tableName, blurQuery);
    for (BlurResult result : results.getResults()) {
      System.out.println(result.getFetchResult().getRowResult().getRow());
    }
    List<String> terms = client.terms(tableName, TEST, "test2", "", (short) 1000);
    assertTrue(terms.isEmpty());
    UserContext.reset();
  }

  private void addReadMaskColumn(RowMutation mutation) {
    List<RecordMutation> recordMutations = mutation.getRecordMutations();
    for (RecordMutation recordMutation : recordMutations) {
      Record record = recordMutation.getRecord();
      record.addToColumns(new Column("test2", UUID.randomUUID().toString()));
      record.addToColumns(new Column("read_mask", "test2|MASK"));
    }
  }

  @Test
  public void testSecurityReadOnlyRowQuery() throws BlurException, TException, IOException {
    String tableName = "testSecurity1";
    createTable(tableName);
    Iface client = getClient();
    String bulkId = UUID.randomUUID().toString();
    client.bulkMutateStart(bulkId);
    for (int i = 0; i < 1000; i++) {
      RowMutation mutation1 = new RowMutation(tableName, getRowId(), RowMutationType.REPLACE_ROW, getRecordMutations(
          "a", "a&b", "(a&b)|c"));
      RowMutation mutation2 = new RowMutation(tableName, getRowId(), RowMutationType.REPLACE_ROW, getRecordMutations(
          "b", "b&c", "a|(b&c)"));
      RowMutation mutation3 = new RowMutation(tableName, getRowId(), RowMutationType.REPLACE_ROW, getRecordMutations(
          "c", "c&a", "(a&c)|b"));
      client.bulkMutateAddMultiple(bulkId, Arrays.asList(mutation1, mutation2, mutation3));
    }
    client.bulkMutateFinish(bulkId, true, true);

    Map<String, String> attributes = new HashMap<String, String>();
    attributes.put(BlurConstants.ACL_READ, "a");
    User user = new User("testuser", attributes);

    UserContext.setUser(user);
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setRowQuery(true);
    query.setQuery("test.test:value");
    blurQuery.setQuery(query);
    blurQuery.setSelector(new Selector());
    BlurResults results = client.query(tableName, blurQuery);
    assertEquals(2000, results.getTotalResults());

    Set<String> aclChecks = new HashSet<String>();
    aclChecks.add("a");
    aclChecks.add("a|(b&c)");

    for (BlurResult result : results.getResults()) {
      FetchResult fetchResult = result.getFetchResult();
      assertNotNull(fetchResult);
      FetchRowResult rowResult = fetchResult.getRowResult();
      assertNotNull(rowResult);
      Row row = rowResult.getRow();
      assertNotNull(row);
      List<Record> records = row.getRecords();
      assertEquals(1, records.size());
      for (Record record : records) {
        Column column = findColumn(record, ACL_READ);
        String value = column.getValue();
        assertTrue(aclChecks.contains(value));
      }
    }

    UserContext.reset();
  }

  private Column findColumn(Record record, String name) {
    for (Column column : record.getColumns()) {
      if (column.getName().equals(name)) {
        return column;
      }
    }
    return null;
  }

  private List<RecordMutation> getRecordMutations(String... readAcls) {
    List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
    for (int i = 0; i < readAcls.length; i++) {
      String recordId = Integer.toString(i);
      RecordMutation recordMutation = BlurThriftHelper.newRecordMutation(TEST, recordId,
          BlurThriftHelper.newColumn(TEST, "value"), BlurThriftHelper.newColumn(ACL_READ, readAcls[i]));
      recordMutations.add(recordMutation);
    }
    return recordMutations;
  }

  private String getRowId() {
    return UUID.randomUUID().toString();
  }

}
