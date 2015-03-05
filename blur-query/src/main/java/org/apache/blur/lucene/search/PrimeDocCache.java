package org.apache.blur.lucene.search;

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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.index.AtomicReaderUtil;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.memory.MemoryLeakDetector;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;

public class PrimeDocCache {

  private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);

  public static final OpenBitSet EMPTY_BIT_SET = new OpenBitSet();

  private static Map<Term, Map<Object, OpenBitSet>> termPrimeDocMap = new ConcurrentHashMap<Term, Map<Object, OpenBitSet>>();

  /**
   * The way this method is called via warm up methods the likelihood of
   * creating multiple bitsets during a race condition is very low, that's why
   * this method is not synced.
   */
  public static OpenBitSet getPrimeDocBitSet(Term primeDocTerm, AtomicReader providedReader) throws IOException {
    AtomicReader reader = AtomicReaderUtil.getSegmentReader(providedReader);
    final Object key = reader.getCoreCacheKey();
    final Map<Object, OpenBitSet> primeDocMap = getPrimeDocMap(primeDocTerm);
    OpenBitSet bitSet = primeDocMap.get(key);
    if (bitSet == null) {
      synchronized (reader) {
        reader.addReaderClosedListener(new ReaderClosedListener() {
          @Override
          public void onClose(IndexReader reader) {
            LOG.debug("Current size [" + primeDocMap.size() + "] Prime Doc BitSet removing for segment [" + reader
                + "]");
            OpenBitSet openBitSet = primeDocMap.remove(key);
            if (openBitSet == null) {
              LOG.warn("Primedoc was missing for key [{0}]", key);
            }
          }
        });
        LOG.debug("Prime Doc BitSet missing for segment [" + reader + "] current size [" + primeDocMap.size() + "]");
        final OpenBitSet bs = new OpenBitSet(reader.maxDoc());
        MemoryLeakDetector.record(bs, "PrimeDoc BitSet", key.toString());

        Fields fields = reader.fields();
        if (fields == null) {
          throw new IOException("Missing all fields.");
        }
        Terms terms = fields.terms(primeDocTerm.field());
        if (terms == null) {
          throw new IOException("Missing prime doc field [" + primeDocTerm.field() + "].");
        }
        TermsEnum termsEnum = terms.iterator(null);
        if (!termsEnum.seekExact(primeDocTerm.bytes(), true)) {
          throw new IOException("Missing prime doc term [" + primeDocTerm + "].");
        }

        DocsEnum docsEnum = termsEnum.docs(null, null);
        int docFreq = reader.docFreq(primeDocTerm);
        int doc;
        int count = 0;
        while ((doc = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          bs.fastSet(doc);
          count++;
        }
        if (count == docFreq) {
          primeDocMap.put(key, bs);
        } else {
          LOG.warn("PrimeDoc for reader [{0}] not stored, because count [{1}] and freq [{2}] do not match.", reader,
              count, docFreq);
        }
        return bs;
      }
    }
    return bitSet;
  }

  private static Map<Object, OpenBitSet> getPrimeDocMap(Term primeDocTerm) {
    Map<Object, OpenBitSet> map = termPrimeDocMap.get(primeDocTerm);
    if (map == null) {
      termPrimeDocMap.put(primeDocTerm, new ConcurrentHashMap<Object, OpenBitSet>());
      return termPrimeDocMap.get(primeDocTerm);
    }
    return map;
  }

}
