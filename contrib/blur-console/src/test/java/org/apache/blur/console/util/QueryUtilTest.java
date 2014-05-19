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

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.blur.console.ConsoleTestBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
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
import org.junit.Before;
import org.junit.Test;

public class QueryUtilTest extends ConsoleTestBase {
	@Before
	public void setup() throws IOException, BlurException, TException {
		setupConfigIfNeeded();
		
		Iface client = BlurClient.getClient(cluster.getControllerConnectionStr());
		
		TableDescriptor td = new TableDescriptor();
		td.setShardCount(11);
		td.setTableUri("file://" + TABLE_PATH + "/queryUnitTable");
		td.setCluster("default");
		td.setName("queryUnitTable");
		td.setEnabled(true);
		client.createTable(td);
		
//		Record record = new Record();
//	    record.setRecordId("abcd");
//	    record.setFamily("fam0");
//	    List<Column> columns = new ArrayList<Column>();
//	    columns.add(new Column("col0", "testvalue"));
//	    record.setColumns(columns);
//
//	    RecordMutation recordMutation = new RecordMutation();
//	    recordMutation.setRecord(record);
//	    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
//
//	    List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
//	    recordMutations.add(recordMutation);
//
//	    RowMutation mutation = new RowMutation();
//	    mutation.setTable("unitTable);
//	    mutation.setRowId(rowid);
//	    mutation.setRowMutationType(RowMutationType.UPDATE_ROW);
//	    mutation.setRecordMutations(recordMutations);
//
//	    client.mutate(mutation);
		
		Record record = new Record("abcd", "fam0", Arrays.asList(new Column[]{ new Column("col0", "testvalue")}));
		RecordMutation recordMutation = new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record);
		RowMutation rowMutation = new RowMutation("queryUnitTable", "12345", RowMutationType.REPLACE_ROW, Arrays.asList(new RecordMutation[]{ recordMutation }));
		client.mutate(rowMutation);
	}
	
	@Test
	public void testGetQueryStatus() throws BlurException, IOException, TException {
		Iface client = BlurClient.getClient(cluster.getControllerConnectionStr());
		QueryUtil.getQueryStatus();
		BlurQuery query = new BlurQuery(
				new Query("fam0.col0:*", true, ScoreType.SUPER, null, null), 
				null, 
				null, //new Selector(false, null, null, null, null, null, 0, 10, null), 
				false, 0, 10, 1, 2000, UUID.randomUUID().toString(), "testUser", false, System.currentTimeMillis(),null,null);
		client.query("queryUnitTable", query);
		
		QueryUtil.getQueryStatus();
	}
}
