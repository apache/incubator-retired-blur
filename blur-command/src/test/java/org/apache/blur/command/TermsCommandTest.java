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
    List<String> returned = getExecuteResult(ctx, "val", null, null);
    List<String> expected = Lists.newArrayList("val");

    assertEquals(expected, returned);
  }

  @Test
  public void sizeOfTermsRequestShouldBeRespected() throws IOException {
    List<String> returned = getExecuteResult(ctx, "alpha", (short) 7, null);
    List<String> expected = Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg");

    assertEquals(expected, returned);
  }

  @Test
  public void sizeShouldDefaultToTen() throws IOException {
    List<String> returned = getExecuteResult(ctx, "alpha", null, null);
    List<String> expected = Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj");

    assertEquals(expected, returned);
  }

  @Test
  public void combineSizeShouldDefaultToTen() throws IOException, InterruptedException {
    Map<Shard, List<String>> execResults = Maps.newHashMap();
    execResults.put(new Shard("t1", "s1"), Lists.newArrayList("aa", "cc", "ee", "gg", "ii"));
    execResults.put(new Shard("t1", "s2"), Lists.newArrayList("bb", "dd", "ff", "hh", "jj"));

    List<String> expected = Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj");

    TermsCommand cmd = new TermsCommand();
    List<String> returned = cmd.combine(new TestCombiningContext(), execResults);

    assertEquals(expected, returned);
  }

  @Test
  public void combineShouldRespectSize() throws IOException, InterruptedException {
    Map<Shard, List<String>> execResults = Maps.newHashMap();
    execResults.put(new Shard("t1", "s1"), Lists.newArrayList("aa", "cc"));
    execResults.put(new Shard("t1", "s2"), Lists.newArrayList("bb", "dd"));

    List<String> expected = Lists.newArrayList("aa", "bb");

    TermsCommand cmd = new TermsCommand();
    cmd.setSize((short) 2);
    List<String> returned = cmd.combine(new TestCombiningContext(), execResults);

    assertEquals(expected, returned);
  }

  @Test
  public void combineEmptyShouldGiveNiceEmptyList() throws IOException, InterruptedException {
    Map<Shard, List<String>> execResults = Maps.newHashMap();
    List<String> expected = Lists.newArrayList();

    TermsCommand cmd = new TermsCommand();
    List<String> returned = cmd.combine(new TestCombiningContext(), execResults);

    assertEquals(expected, returned);
  }

  private List<String> getExecuteResult(IndexContext context, String field, Short size, String startsWith)
      throws IOException {
    TermsCommand cmd = new TermsCommand();
    cmd.setFieldName(field);
    if (startsWith != null) {
      cmd.setStartWith(startsWith);
    }
    if (size != null) {
      cmd.setSize(size);
    }
    return cmd.execute(context);
  }
}
