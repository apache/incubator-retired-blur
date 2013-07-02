package org.apache.blur.manager.results;

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
import java.util.Comparator;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;

public class BlurResultPeekableIteratorComparator implements Comparator<PeekableIterator<BlurResult, BlurException>> {

  @Override
  public int compare(PeekableIterator<BlurResult, BlurException> o1, PeekableIterator<BlurResult, BlurException> o2) {
    BlurResult result1 = o1.peek();
    BlurResult result2 = o2.peek();
    if (result1 == null && result2 == null) {
      return 0;
    } else if (result1 == null) {
      return 1;
    } else if (result2 == null) {
      return -1;
    }
    int compare = Double.compare(result2.score, result1.score);
    if (compare == 0) {
      return result2.locationId.compareTo(result1.locationId);
    }
    return compare;
  }

}
