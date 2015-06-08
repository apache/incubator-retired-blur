package org.apache.blur.thrift;
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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TermsTestsIT extends BaseClusterTest {
  
  @Test
  public void testTermsList() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testTermsList";
    TableGen.define(tableName).cols("test", "col1")
        .addRows(100, 20, "r1", "rec-###", "value").build(getClient());

    List<String> terms = getClient().terms(tableName, "test", "col1", null, (short) 10);
    List<String> list = Lists.newArrayList("value");
    
    assertEquals(list, terms);
  }
  
  @Test
  public void shouldBeAbleToSkipToTerms() throws Exception {
    final String tableName = "shouldBeAbleToNavigateTerms";
    TableGen.define(tableName)
      .cols("test", "col1")
      .addRecord("1","1", "aaa")
      .addRecord("2","2", "bbb")
      .addRecord("3","3", "ccc")
      .addRecord("4","4", "ddd")
      .build(getClient());
    
    List<String> terms = getClient().terms(tableName, "test", "col1", "c", (short)10);
    List<String> expected = Lists.newArrayList("ccc", "ddd");
    
    assertEquals(expected, terms);
  }
  
  @Test
  public void shouldOnlyReturnNumberOfTermsRequested() throws Exception {
    final String tableName = "shouldOnlyReturnNumberOfTermsRequested";
    TableGen.define(tableName)
      .cols("test", "col1")
      .addRecord("1","1", "aaa")
      .addRecord("2","2", "bbb")
      .addRecord("3","3", "ccc")
      .addRecord("4","4", "ddd")
      .build(getClient());
    
    List<String> terms = getClient().terms(tableName, "test", "col1", "c", (short)1);
    List<String> expected = Lists.newArrayList("ccc");
    
    assertEquals(expected, terms);
  }
  
  @Test
  public void shouldGetEmptyListForNonExistentTerms() throws Exception {
    final String tableName = "shouldGetEmptyListForNonExistantTerms";
    TableGen.define(tableName)
      .cols("test", "col1")
      .addRecord("1","1", "aaa")
      .addRecord("2","2", "bbb")
      .addRecord("3","3", "ccc")
      .addRecord("4","4", "ddd")
      .build(getClient());
    
    List<String> terms = getClient().terms(tableName, "test", "col1", "z", (short)1);
    assertNotNull(terms);
    assertEquals(0, terms.size());
  }
  
  @Test(expected=BlurException.class)
  public void termsShouldFailOnUnknownTable() throws BlurException, TException, IOException {
    getClient().terms("termsShouldFailOnUnknownTable", "test","col1", null, (short)10);
  }

}
