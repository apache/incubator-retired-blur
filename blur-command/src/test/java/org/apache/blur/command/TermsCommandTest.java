package org.apache.blur.command;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

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
    List<String> returned = getExecuteResult(newContext("val", null, null));
    List<String> expected = Lists.newArrayList("val");

    assertEquals(expected, returned);
  }

  @Test
  public void sizeOfTermsRequestShouldBeRespected() throws IOException {
    List<String> returned = getExecuteResult(newContext("alpha", (short) 7, null));
    List<String> expected = Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg");

    assertEquals(expected, returned);
  }

  @Test
  public void sizeShouldDefaultToTen() throws IOException {
    List<String> returned = getExecuteResult(newContext("alpha", null, null));
    List<String> expected = Lists.newArrayList("aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj");

    assertEquals(expected, returned);
  }

  private List<String> getExecuteResult(IndexContext context) throws IOException {
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
