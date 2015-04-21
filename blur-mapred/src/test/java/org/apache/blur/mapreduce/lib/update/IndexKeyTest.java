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
package org.apache.blur.mapreduce.lib.update;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.blur.mapreduce.lib.update.IndexKey;
import org.junit.Test;

public class IndexKeyTest {

  @Test
  public void testIndexKey() {
    Random random = new Random();
    long seed = random.nextLong();

    List<IndexKey> keys = new ArrayList<IndexKey>();
    keys.add(IndexKey.newDataMarker("row1"));
    keys.add(IndexKey.oldData("row1", "record1"));
    keys.add(IndexKey.newData("row1", "record1", 1L));
    keys.add(IndexKey.newData("row1", "record1", 2L));
    keys.add(IndexKey.oldData("row1", "record2"));
    keys.add(IndexKey.newData("row1", "record3", 2L));
    keys.add(IndexKey.oldData("row1", "record4"));
    keys.add(IndexKey.oldData("row2", "record1"));

    List<IndexKey> copyKeys = new ArrayList<IndexKey>(keys);
    Collections.shuffle(copyKeys, new Random(seed));

    Collections.sort(copyKeys);

    for (int i = 0; i < keys.size(); i++) {
      assertEquals("Failed with seed [" + seed + "]", keys.get(i), copyKeys.get(i));
    }
  }
}
