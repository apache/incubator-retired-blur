package org.apache.blur.command;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TermsCommandTest {
  private static IndexContext ctx;

  @BeforeClass
  public static void init() {
    ctx = CoreTestContext.newSimpleAlpaNumContext();
  }

  @Test
  public void basicTermsShouldReturn() throws IOException {
    BlurArray returned = getExecuteResult(newContext("val", null, null));
    BlurArray expected = new BlurArray(Lists.newArrayList("val"));

    assertEquals(expected, returned);
  }

  @Test
  public void sizeOfTermsRequestShouldBeRespected() throws IOException {
    BlurArray returned = getExecuteResult(newContext("alpha", (short) 7, null));
    BlurArray expected = new BlurArray(Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg"));

    assertEquals(expected, returned);
  }

  @Test
  public void sizeShouldDefaultToTen() throws IOException {
    BlurArray returned = getExecuteResult(newContext("alpha", null, null));
    BlurArray expected = new BlurArray(Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"));

    assertEquals(expected, returned);
  }
  
  @Test
  public void combineSizeShouldDefaultToTen() throws IOException, InterruptedException {
    Map<Shard, BlurArray> execResults = Maps.newHashMap();
    execResults.put(new Shard("t1", "s1"), new BlurArray(Lists.newArrayList("aa", "cc", "ee", "gg", "ii")));
    execResults.put(new Shard("t1", "s2"), new BlurArray(Lists.newArrayList("bb", "dd", "ff", "hh", "jj")));
    
    BlurArray expected = new BlurArray(Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj"));
    
    TermsCommand cmd = new TermsCommand();
    Args args = new Args();
    BlurObject params = new BlurObject();
    args.set("params", params);
    BlurArray returned = cmd.combine(new TestCombiningContext(args), execResults);
    
    assertEquals(expected, returned);
  }

  @Test
  public void combineShouldRespectSize() throws IOException, InterruptedException {
    Map<Shard, BlurArray> execResults = Maps.newHashMap();
    execResults.put(new Shard("t1", "s1"), new BlurArray(Lists.newArrayList("aa", "cc")));
    execResults.put(new Shard("t1", "s2"), new BlurArray(Lists.newArrayList("bb", "dd")));
    
    BlurArray expected = new BlurArray(Lists.newArrayList("aa", "bb"));
    
    TermsCommand cmd = new TermsCommand();
    Args args = new Args();
    BlurObject params = new BlurObject();
    params.put("size", (short)2);
    args.set("params", params);
    
    BlurArray returned = cmd.combine(new TestCombiningContext(args), execResults);
    
    assertEquals(expected, returned);
  }
  
  @Test
  public void combineEmptyShouldGiveNiceEmptyList() throws IOException, InterruptedException {
    Map<Shard, BlurArray> execResults = Maps.newHashMap();
    BlurArray expected = new BlurArray(Lists.newArrayList());
    
    TermsCommand cmd = new TermsCommand();
    BlurArray returned = cmd.combine(new TestCombiningContext(), execResults);
    
    assertEquals(expected, returned);
  }
  
  
  private BlurArray getExecuteResult(IndexContext context) throws IOException {
    TermsCommand cmd = new TermsCommand();
    return cmd.execute(context);
  }
  
  private IndexContext newContext(String field, Short size, String startsWith) {

    Args args = new Args();
    BlurObject params = new BlurObject();
    params.put("fieldName", field);

    if (size != null) {
      params.put("size", size);
    }
    if (startsWith != null) {
      params.put("startWith", startsWith);
    }
    args.set("params", params);

    return new TestContextArgDecorator(ctx, args);
  }
}
