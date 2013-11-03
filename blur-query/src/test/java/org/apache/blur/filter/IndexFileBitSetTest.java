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
package org.apache.blur.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.junit.Test;

public class IndexFileBitSetTest {

  private long _seed;

  @Before
  public void setup() {
    Random random = new Random();
    _seed = random.nextLong();
  }

  @Test
  public void test() throws IOException {
    Random random = new Random(_seed);
    int numBits = random.nextInt(10000000);
    FixedBitSet fixedBitSet = new FixedBitSet(numBits);
    populate(random, numBits, fixedBitSet);
    String id = "id";
    String segmentName = "seg1";
    RAMDirectory directory = new RAMDirectory();
    IndexFileBitSet indexFileBitSet = new IndexFileBitSet(numBits, id, segmentName, directory);
    assertFalse(indexFileBitSet.exists());
    indexFileBitSet.create(fixedBitSet.iterator());
    indexFileBitSet.load();
    checkEquals(fixedBitSet.iterator(), indexFileBitSet.iterator(), numBits);
    indexFileBitSet.close();
    
    String[] listAll = directory.listAll();
    for (String s : listAll) {
      System.out.println(s + " " + directory.fileLength(s));
    }
  }

  private void populate(Random random, int numBits, FixedBitSet fixedBitSet) {
    int population = random.nextInt(numBits);
    for (int i = 0; i < population; i++) {
      fixedBitSet.set(random.nextInt(numBits));
    }
  }

  private void checkEquals(DocIdSetIterator expected, DocIdSetIterator actual, int numBits) throws IOException {
    int expectedNextDoc;
    while ((expectedNextDoc = expected.nextDoc()) < numBits) {
      int actualNextDoc = actual.nextDoc();
      assertEquals(expectedNextDoc, actualNextDoc);
    }
  }

}
