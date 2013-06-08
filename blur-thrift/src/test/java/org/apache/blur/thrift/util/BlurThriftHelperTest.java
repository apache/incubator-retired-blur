package org.apache.blur.thrift.util;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.junit.Test;

public class BlurThriftHelperTest {

  @Test
  public void testRecordMatch() {
    Record r1 = BlurThriftHelper.newRecord("test-family", "record-1", BlurThriftHelper.newColumn("a", "b"));
    Record r2 = BlurThriftHelper.newRecord("test-family", "record-1", BlurThriftHelper.newColumn("c", "d"));
    Record r3 = BlurThriftHelper.newRecord("test-family", "record-2", BlurThriftHelper.newColumn("e", "f"));
    Record r4 = BlurThriftHelper.newRecord("test-family-2", "record-1", BlurThriftHelper.newColumn("g", "h"));

    assertTrue("should match with same family and record-id", BlurThriftHelper.match(r1, r2));
    assertFalse("should not match with different record-id", BlurThriftHelper.match(r1, r3));
    assertFalse("should not match with different family", BlurThriftHelper.match(r1, r4));
  }

  @Test
  public void testRecordMutationMatch() {
    RecordMutation rm1 = BlurThriftHelper.newRecordMutation("test-family", "record-1",
        BlurThriftHelper.newColumn("a", "b"));
    RecordMutation rm2 = BlurThriftHelper.newRecordMutation("test-family", "record-2",
        BlurThriftHelper.newColumn("c", "d"));
    RecordMutation rm3 = BlurThriftHelper.newRecordMutation("test-family-2", "record-1",
        BlurThriftHelper.newColumn("e", "f"));
    Record r = BlurThriftHelper.newRecord("test-family", "record-1", BlurThriftHelper.newColumn("g", "h"));

    assertTrue("should match with same family and record-id", BlurThriftHelper.match(rm1, r));
    assertFalse("should not match with different record-id", BlurThriftHelper.match(rm2, r));
    assertFalse("should not match with different family", BlurThriftHelper.match(rm3, r));
  }

  @Test
  public void testFindRecordMutation() {
    RecordMutation rm1 = BlurThriftHelper.newRecordMutation("test-family", "record-1",
        BlurThriftHelper.newColumn("a", "b"));
    RecordMutation rm2 = BlurThriftHelper.newRecordMutation("test-family", "record-2",
        BlurThriftHelper.newColumn("c", "d"));
    RecordMutation rm3 = BlurThriftHelper.newRecordMutation("test-family-2", "record-1",
        BlurThriftHelper.newColumn("e", "f"));
    RowMutation row = BlurThriftHelper.newRowMutation("test-table", "row-123", rm1, rm2, rm3);
    Record r = BlurThriftHelper.newRecord("test-family", "record-2", BlurThriftHelper.newColumn("g", "h"));
    Record r2 = BlurThriftHelper.newRecord("test-family", "record-99", BlurThriftHelper.newColumn("g", "h"));

    assertEquals("should find record-2", rm2, BlurThriftHelper.findRecordMutation(row, r));
    assertNull("should not find record-99", BlurThriftHelper.findRecordMutation(row, r2));
  }

}
