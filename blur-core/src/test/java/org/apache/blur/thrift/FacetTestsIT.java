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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResults;
import org.junit.Test;

public class FacetTestsIT extends BaseClusterTest {

  @Test
  public void testQueryWithFacets() throws BlurException, TException, IOException, InterruptedException {
    String table = "testQueryWithFacets";
    TableGen.define(table)
      .cols("test", "string:facet")
      .addRow(20, "r1", "rec###", "value-###")
      .build(getClient());
    
    QueryBuilder qb = QueryBuilder.define("*");
    for (int i = 0; i < 250; i++) {
      qb.withFacet("test.facet:" + i, Long.MAX_VALUE);
    }

    BlurResults resultsRow = qb.run(table, getClient());
    assertEquals(1, resultsRow.getTotalResults());
  }

  
  @Test
  public void testQueryWithFacetsWithMins() throws BlurException, TException, IOException, InterruptedException {
    final String tableName = "testQueryWithFacetsWithMins";
    int numberOfRows = 100;
    
    TableGen.define(tableName)
    .cols("test", "string:facet", "string:facetFixed")
    .addRows(numberOfRows, 20, "r1", "rec###", "value-###", "test")
    .build(getClient());
    
    
    BlurResults resultsRow = QueryBuilder.define("*")
        .withFacet("test.facetFixed:test", 50)
        .run(tableName, getClient());

    assertEquals(numberOfRows, resultsRow.getTotalResults());

    List<Long> facetCounts = resultsRow.getFacetCounts();
    for (Long l : facetCounts) {
      assertTrue(l >= 50);
    }
  }

}
