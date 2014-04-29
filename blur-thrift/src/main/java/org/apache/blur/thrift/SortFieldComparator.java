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
package org.apache.blur.thrift;

import java.util.Comparator;

import org.apache.blur.thrift.generated.SortFieldResult;
import org.apache.blur.thrift.generated.SortFieldResult._Fields;

public class SortFieldComparator implements Comparator<SortFieldResult> {

  @Override
  public int compare(SortFieldResult o1, SortFieldResult o2) {
    _Fields field = o1.getSetField();
    int lastComparison = org.apache.blur.thirdparty.thrift_0_9_0.TBaseHelper.compareTo(field, o2.getSetField());
    if (lastComparison == 0) {
      Object obj1 = o1.getFieldValue();
      Object obj2 = o2.getFieldValue();
      switch (field) {
      case NULL_VALUE:
        // if both are null type they are equal.
        return 0;
      case STRING_VALUE:
        return ((String) obj1).compareTo((String) obj2);
      case LONG_VALUE: // INT_VALUE
        return ((Long) obj1).compareTo((Long) obj2);
      case DOUBLE_VALUE: // LONG_VALUE
        return ((Double) obj1).compareTo((Double) obj2);
      case INT_VALUE: // DOUBLE_VALUE
        return ((Integer) obj1).compareTo((Integer) obj2);
      case BINARY_VALUE: // BINARY_VALUE
        return compare((byte[]) obj1, (byte[]) obj2);
      default:
        throw new RuntimeException("Unsupported type of [" + field + "]");
      }
    }
    return lastComparison;
  }

  public int compare(byte[] b1, byte[] b2) {
    return compareBytes(b1, 0, b1.length, b2, 0, b2.length);
  }

  public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int end1 = s1 + l1;
    int end2 = s2 + l2;
    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
      int a = (b1[i] & 0xff);
      int b = (b2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

}
