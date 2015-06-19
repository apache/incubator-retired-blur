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
package org.apache.blur.doc;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class BlurPropertyParserTest {
  private BlurPropertyParser parser;

  @Before
  public void setUp() throws Exception {
    parser = new BlurPropertyParser();
  }

  @Test
  public void testDefaultValNull() {
    assertEquals("string", parser.getType(""));
    assertEquals("string", parser.getType(null));
  }

  @Test
  public void testDefaultValInt() {
    assertEquals("long", parser.getType(Integer.toString(Integer.MIN_VALUE)));
    assertEquals("long", parser.getType(Integer.toString(Integer.MAX_VALUE)));
  }

  @Test
  public void testDefaultValBoolean() {
    assertEquals("boolean", parser.getType("true"));
    assertEquals("boolean", parser.getType("false"));
  }

  @Test
  public void testDefaultValDouble() {
    assertEquals("double", parser.getType("0.75"));
    assertEquals("double", parser.getType("-0.75"));
  }

}
