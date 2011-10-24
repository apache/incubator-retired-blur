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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.nearinfinity.blur.manager.results.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurResultPeekableIteratorComparatorTest {

  @Test
  public void testResultPeekableIteratorComparator() {
    List<PeekableIterator<BlurResult>> results = new ArrayList<PeekableIterator<BlurResult>>();
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>(Arrays.asList(newResult("5", 5))).iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>().iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>().iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>(Arrays.asList(newResult("2", 2))).iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>(Arrays.asList(newResult("1", 1))).iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>(Arrays.asList(newResult("9", 1))).iterator()));
    results.add(new PeekableIterator<BlurResult>(new ArrayList<BlurResult>().iterator()));

    Collections.sort(results, BlurConstants.HITS_PEEKABLE_ITERATOR_COMPARATOR);

    for (PeekableIterator<BlurResult> iterator : results) {
      System.out.println(iterator.peek());
    }
  }

  private BlurResult newResult(String id, double score) {
    return new BlurResult(id, score, null);
  }

}
