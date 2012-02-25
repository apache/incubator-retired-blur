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

package com.nearinfinity.blur.utils;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderFinishedListener;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.OpenBitSet;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class PrimeDocCache {

  private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);

  public static final OpenBitSet EMPTY_BIT_SET = new OpenBitSet();
  private static final Term PRIME_DOC_TERM = new Term(PRIME_DOC, PRIME_DOC_VALUE);
  
  private static Map<IndexReader,OpenBitSet> primeDocMap = new ConcurrentHashMap<IndexReader, OpenBitSet>();

  public static synchronized OpenBitSet getPrimeDocBitSet(IndexReader reader) throws IOException {
    OpenBitSet bitSet = primeDocMap.get(reader);
    if (bitSet == null) {
      reader.addReaderFinishedListener(new ReaderFinishedListener() {
        @Override
        public void finished(IndexReader reader) {
          primeDocMap.remove(reader);
        }
      });
      LOG.info("Prime Doc BitSet missing for segment [" + reader + "]");
      bitSet = new OpenBitSet(reader.maxDoc());
      primeDocMap.put(reader,bitSet);
      TermDocs termDocs = reader.termDocs(PRIME_DOC_TERM);
      while (termDocs.next()) {
        bitSet.set(termDocs.doc());
      }
      termDocs.close();
    }
    return bitSet;
  }

}
