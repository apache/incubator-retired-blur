/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.results;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.nearinfinity.blur.manager.results.PeekableIterator;

public class PeekableIteratorTest {

  @Test
  public void testPeekableIterator1() {
    PeekableIterator<Integer> iterator = new PeekableIterator<Integer>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).iterator());
    while (iterator.hasNext()) {
      for (int i = 0; i < 3; i++) {
        System.out.println(iterator.peek());
      }
      System.out.println(iterator.next());
    }
  }

  @Test
  public void testPeekableIteratorEmpty() {
    PeekableIterator<Integer> iterator = new PeekableIterator<Integer>(new ArrayList<Integer>().iterator());
    for (int i = 0; i < 3; i++) {
      System.out.println(iterator.peek());
    }
    while (iterator.hasNext()) {
      for (int i = 0; i < 3; i++) {
        System.out.println(iterator.peek());
      }
      System.out.println(iterator.next());
    }
  }

}
