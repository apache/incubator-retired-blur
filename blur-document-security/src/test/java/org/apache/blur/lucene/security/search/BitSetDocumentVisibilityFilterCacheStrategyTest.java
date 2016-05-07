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
package org.apache.blur.lucene.security.search;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;

public class BitSetDocumentVisibilityFilterCacheStrategyTest {

  @Test
  public void testIdFullySet1() {
    int maxDoc = 10;
    OpenBitSet bitSet = new OpenBitSet(maxDoc);
    assertFalse(BitSetDocumentVisibilityFilterCacheStrategy.isFullySet(maxDoc, bitSet, bitSet.cardinality()));
  }

  @Test
  public void testIdFullySet2() {
    int maxDoc = 10;
    OpenBitSet bitSet = new OpenBitSet(maxDoc);
    for (int d = 0; d < maxDoc; d++) {
      bitSet.set(d);
    }
    assertTrue(BitSetDocumentVisibilityFilterCacheStrategy.isFullySet(maxDoc, bitSet, bitSet.cardinality()));
  }

  @Test
  public void testIdFullyEmpty1() {
    int maxDoc = 10;
    OpenBitSet bitSet = new OpenBitSet(maxDoc);
    assertTrue(BitSetDocumentVisibilityFilterCacheStrategy.isFullyEmpty(bitSet, bitSet.cardinality()));
  }

  @Test
  public void testIdFullyEmpty2() {
    int maxDoc = 10;
    OpenBitSet bitSet = new OpenBitSet(maxDoc);
    bitSet.set(3);
    assertFalse(BitSetDocumentVisibilityFilterCacheStrategy.isFullyEmpty(bitSet, bitSet.cardinality()));
  }

  @Test
  public void testFullySetDocIdSet() throws IOException {
    int len = 10;
    DocIdSet docIdSet = BitSetDocumentVisibilityFilterCacheStrategy.getFullySetDocIdSet(len);
    Bits bits = docIdSet.bits();
    assertEquals(len, bits.length());
    for (int i = 0; i < len; i++) {
      assertTrue(bits.get(i));
    }
    assertTrue(docIdSet.isCacheable());
    {
      DocIdSetIterator iterator = docIdSet.iterator();
      int adoc;
      int edoc = 0;
      assertEquals(-1, iterator.docID());
      while ((adoc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        assertEquals(edoc, adoc);
        assertEquals(edoc, iterator.docID());
        edoc++;
      }
      assertEquals(len, edoc);
    }
    {
      DocIdSetIterator iterator = docIdSet.iterator();
      int adoc;
      int edoc = 0;
      assertEquals(-1, iterator.docID());
      while ((adoc = iterator.advance(edoc)) != DocIdSetIterator.NO_MORE_DOCS) {
        assertEquals(edoc, adoc);
        assertEquals(edoc, iterator.docID());
        edoc++;
      }
      assertEquals(len, edoc);
    }
  }

  @Test
  public void testFullyEmptyDocIdSet() throws IOException {
    int len = 10;
    DocIdSet docIdSet = BitSetDocumentVisibilityFilterCacheStrategy.getFullyEmptyDocIdSet(len);
    Bits bits = docIdSet.bits();
    assertEquals(len, bits.length());
    for (int i = 0; i < len; i++) {
      assertFalse(bits.get(i));
    }
    assertTrue(docIdSet.isCacheable());
    {
      DocIdSetIterator iterator = docIdSet.iterator();
      assertEquals(-1, iterator.docID());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }
    {
      DocIdSetIterator iterator = docIdSet.iterator();
      assertEquals(-1, iterator.docID());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(0));
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
    }
  }
}
