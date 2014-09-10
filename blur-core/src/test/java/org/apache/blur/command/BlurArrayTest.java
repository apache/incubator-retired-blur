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
package org.apache.blur.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.blur.command.BlurArray;
import org.junit.Test;

public class BlurArrayTest {

  @Test
  public void testPutAndLength() {
    BlurArray array = new BlurArray();
    assertEquals(0, array.length());
    array.put("v");
    assertEquals(1, array.length());
    assertEquals("v", array.getObject(0));
  }

  @Test
  public void testArrayOutOfBounds() {
    BlurArray array = new BlurArray();
    try {
      array.getObject(123);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Pass
    }
  }

  @Test
  public void testSettingValues() {
    BlurArray array = new BlurArray();
    array.put(1, "a");
    assertEquals("a", array.getObject(1));
    assertEquals(2, array.length());
    assertNull(array.getObject(0));
    array.put(1, "b");
    assertEquals("b", array.getObject(1));
    assertEquals(2, array.length());
    assertNull(array.getObject(0));
  }

}
