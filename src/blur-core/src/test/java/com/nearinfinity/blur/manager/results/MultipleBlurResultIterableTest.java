package com.nearinfinity.blur.manager.results;

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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.results.BlurResultIterableMultiple;
import com.nearinfinity.blur.manager.results.BlurResultIterableSimple;
import com.nearinfinity.blur.thrift.generated.BlurResult;

public class MultipleBlurResultIterableTest {

  @Test
  public void testMultipleHitsIterable() {
    BlurResultIterableMultiple iterable = new BlurResultIterableMultiple();
    iterable.addBlurResultIterable(newBlurResultIterable(0, 0.1, 3, 2, 9, 10, 2));
    iterable.addBlurResultIterable(newBlurResultIterable(7, 2, 9, 1, 34, 53, 12));
    iterable.addBlurResultIterable(newBlurResultIterable(4, 3));
    iterable.addBlurResultIterable(newBlurResultIterable(7, 2, 34, 132));
    iterable.addBlurResultIterable(newBlurResultIterable());

    for (BlurResult hit : iterable) {
      System.out.println(hit);
    }
  }

  private BlurResultIterable newBlurResultIterable(double... ds) {
    List<BlurResult> results = new ArrayList<BlurResult>();
    for (double d : ds) {
      results.add(new BlurResult(UUID.randomUUID().toString() + "-" + Double.toString(d), d, null));
    }
    return new BlurResultIterableSimple(UUID.randomUUID().toString(), results);
  }

}
