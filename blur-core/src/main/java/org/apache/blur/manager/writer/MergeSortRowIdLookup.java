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
package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class MergeSortRowIdLookup {

  public interface Action {
    void found(AtomicReader reader, Bits liveDocs, TermsEnum termsEnum) throws IOException;
  }

  private final List<TermsEnumReader> _termsEnumList = new ArrayList<TermsEnumReader>();

  public MergeSortRowIdLookup(IndexReader indexReader) throws IOException {
    if (indexReader instanceof AtomicReader) {
      addAtomicReader((AtomicReader) indexReader);
    } else {
      for (AtomicReaderContext context : indexReader.leaves()) {
        addAtomicReader(context.reader());
      }
    }
  }

  private void addAtomicReader(AtomicReader atomicReader) throws IOException {
    Terms terms = atomicReader.fields().terms(BlurConstants.ROW_ID);
    TermsEnum termsEnum = terms.iterator(null);
    _termsEnumList.add(new TermsEnumReader(termsEnum, atomicReader));
  }

  public void lookup(BytesRef rowId, Action action) throws IOException {
    advance(_termsEnumList, rowId);
    sort(_termsEnumList);
    for (TermsEnumReader reader : _termsEnumList) {
      if (reader._termsEnum.term().equals(rowId)) {
        action.found(reader._reader, reader._liveDocs, reader._termsEnum);
      }
    }
  }

  private static void advance(List<TermsEnumReader> termsEnumList, BytesRef rowId) throws IOException {
    for (TermsEnumReader reader : termsEnumList) {
      BytesRef term = reader._termsEnum.term();
      if (term.compareTo(rowId) < 0) {
        reader._termsEnum.seekCeil(rowId);
      }
    }
  }

  private static void sort(List<TermsEnumReader> termsEnumList) {
    Collections.sort(termsEnumList);
  }

  private static class TermsEnumReader implements Comparable<TermsEnumReader> {

    final Bits _liveDocs;
    final TermsEnum _termsEnum;
    final AtomicReader _reader;

    TermsEnumReader(TermsEnum termsEnum, AtomicReader reader) {
      _termsEnum = termsEnum;
      _reader = reader;
      _liveDocs = reader.getLiveDocs();
    }

    @Override
    public int compareTo(TermsEnumReader o) {
      try {
        BytesRef t1 = _termsEnum.term();
        BytesRef t2 = o._termsEnum.term();
        return t1.compareTo(t2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
