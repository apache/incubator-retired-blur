package org.apache.blur.command;

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
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class DocumentCountCombinerTest  {
  private static IndexContext ctx;
  
  @BeforeClass
  public static void init() {
    ctx = CoreTestContext.newSimpleAlpaNumContext();
  }

  @Test
  public void documentCountShouldBeAccurate() throws IOException {
    DocumentCountCombiner dc = new DocumentCountCombiner();
    
    int docCount = dc.execute(ctx);
   
    assertEquals(26, docCount);
  }
  
  @Test 
  public void combineShouldProperlySum() throws IOException {
    DocumentCountCombiner dc = new DocumentCountCombiner();
    Map<Shard, Integer>  shardTotals = Maps
        .newHashMap(ImmutableMap
            .of(new Shard("t1","s1"), 10, new Shard("t1","s2"), 20, new Shard("t1","s3"), 30));
    long total = dc.combine(new TestCombiningContext(), shardTotals);
    
    assertEquals(60l, total);
  }
  
}
