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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.OpenBitSet;

public class PrimeDocCache {

  private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);

  public static final OpenBitSet EMPTY_BIT_SET = new OpenBitSet();

  private static Map<Term,Map<Object, OpenBitSet>> termPrimeDocMap = new ConcurrentHashMap<Term, Map<Object,OpenBitSet>>();

  /**
   * The way this method is called via warm up methods the likelihood of
   * creating multiple bitsets during a race condition is very low, that's why
   * this method is not synced.
   */
  public static OpenBitSet getPrimeDocBitSet(Term primeDocTerm, IndexReader reader) throws IOException {
    Object key = reader.getCoreCacheKey();
    final Map<Object, OpenBitSet> primeDocMap = getPrimeDocMap(primeDocTerm);
    OpenBitSet bitSet = primeDocMap.get(key);
    if (bitSet == null) {
      reader.addReaderClosedListener(new ReaderClosedListener() {
        @Override
        public void onClose(IndexReader reader) {
          Object key = reader.getCoreCacheKey();
          LOG.debug("Current size [" + primeDocMap.size() + "] Prime Doc BitSet removing for segment [" + reader + "]");
          primeDocMap.remove(key);
        }
      });
      LOG.debug("Prime Doc BitSet missing for segment [" + reader + "] current size [" + primeDocMap.size() + "]");
      final OpenBitSet bs = new OpenBitSet(reader.maxDoc());
      primeDocMap.put(key, bs);
      IndexSearcher searcher = new IndexSearcher(reader);
      searcher.search(new TermQuery(primeDocTerm), new Collector() {

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        public void setNextReader(AtomicReaderContext atomicReaderContext) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
          bs.set(doc);
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return false;
        }
      });
      return bs;
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
