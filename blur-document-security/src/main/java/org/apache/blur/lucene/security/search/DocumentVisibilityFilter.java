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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.blur.lucene.security.DocumentAuthorizations;
import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.blur.lucene.security.DocumentVisibilityEvaluator;
import org.apache.blur.lucene.security.search.DocumentVisibilityFilterCacheStrategy.Builder;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class DocumentVisibilityFilter extends Filter {

  private static final Comparator<DocIdSetIterator> COMPARATOR = new Comparator<DocIdSetIterator>() {
    @Override
    public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
      int docID1 = o1.docID();
      int docID2 = o2.docID();
      return docID1 - docID2;
    }
  };

  private final String _fieldName;
  private final DocumentAuthorizations _authorizations;
  private final DocumentVisibilityFilterCacheStrategy _filterCacheStrategy;

  public DocumentVisibilityFilter(String fieldName, DocumentAuthorizations authorizations,
      DocumentVisibilityFilterCacheStrategy filterCacheStrategy) {
    _fieldName = fieldName;
    _authorizations = authorizations;
    _filterCacheStrategy = filterCacheStrategy;
  }

  @Override
  public String toString() {
    return "DocumentVisibilityFilter [_fieldName=" + _fieldName + ", _authorizations=" + _authorizations
        + ", _filterCacheStrategy=" + _filterCacheStrategy + "]";
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    AtomicReader reader = context.reader();
    List<DocIdSet> list = new ArrayList<DocIdSet>();

    Fields fields = reader.fields();
    Terms terms = fields.terms(_fieldName);
    if (terms == null) {
      // if field is not present then show nothing.
      return DocIdSet.EMPTY_DOCIDSET;
    }
    TermsEnum iterator = terms.iterator(null);
    BytesRef bytesRef;
    DocumentVisibilityEvaluator visibilityEvaluator = new DocumentVisibilityEvaluator(_authorizations);
    while ((bytesRef = iterator.next()) != null) {
      if (isVisible(visibilityEvaluator, bytesRef)) {
        DocIdSet docIdSet = _filterCacheStrategy.getDocIdSet(_fieldName, bytesRef, reader);
        if (docIdSet != null) {
          list.add(docIdSet);
        } else {
          // Do not use acceptDocs because we want the acl cache to be version
          // agnostic.
          DocsEnum docsEnum = iterator.docs(null, null);
          list.add(buildCache(reader, docsEnum, bytesRef));
        }
      }
    }
    return getLogicalOr(list);
  }

  private DocIdSet buildCache(AtomicReader reader, DocIdSetIterator it, BytesRef bytesRef) throws IOException {
    Builder builder = _filterCacheStrategy.createBuilder(_fieldName, bytesRef, reader);
    builder.or(it);
    return builder.getDocIdSet();
  }

  private boolean isVisible(DocumentVisibilityEvaluator visibilityEvaluator, BytesRef bytesRef) throws IOException {
    DocumentVisibility visibility = new DocumentVisibility(trim(bytesRef));
    return visibilityEvaluator.evaluate(visibility);
  }

  private byte[] trim(BytesRef bytesRef) {
    byte[] buf = new byte[bytesRef.length];
    System.arraycopy(bytesRef.bytes, bytesRef.offset, buf, 0, bytesRef.length);
    return buf;
  }

  public static DocIdSet getLogicalOr(DocIdSet... list) throws IOException {
    return getLogicalOr(Arrays.asList(list));
  }

  public static DocIdSet getLogicalOr(final List<DocIdSet> list) throws IOException {
    if (list.size() == 0) {
      return DocIdSet.EMPTY_DOCIDSET;
    }
    if (list.size() == 1) {
      DocIdSet docIdSet = list.get(0);
      Bits bits = docIdSet.bits();
      if (bits == null) {
        throw new IOException("Bits are not allowed to be null for DocIdSet [" + docIdSet + "].");
      }
      return docIdSet;
    }
    int index = 0;
    final Bits[] bitsArray = new Bits[list.size()];
    int length = -1;
    for (DocIdSet docIdSet : list) {
      Bits bits = docIdSet.bits();
      if (bits == null) {
        throw new IOException("Bits are not allowed to be null for DocIdSet [" + docIdSet + "].");
      }
      bitsArray[index] = bits;
      index++;
      if (length < 0) {
        length = bits.length();
      } else if (length != bits.length()) {
        throw new IOException("Bits length need to be the same [" + length + "] and [" + bits.length() + "]");
      }
    }
    final int len = length;
    return new DocIdSet() {

      @Override
      public Bits bits() throws IOException {
        return new Bits() {

          @Override
          public boolean get(int index) {
            for (int i = 0; i < bitsArray.length; i++) {
              if (bitsArray[i].get(index)) {
                return true;
              }
            }
            return false;
          }

          @Override
          public int length() {
            return len;
          }

        };
      }

      @Override
      public boolean isCacheable() {
        return true;
      }

      @Override
      public DocIdSetIterator iterator() throws IOException {
        final DocIdSetIterator[] docIdSetIteratorArray = new DocIdSetIterator[list.size()];
        long c = 0;
        int index = 0;
        for (DocIdSet docIdSet : list) {
          DocIdSetIterator iterator = docIdSet.iterator();
          iterator.nextDoc();
          docIdSetIteratorArray[index] = iterator;
          c += iterator.cost();
          index++;
        }
        final long cost = c;
        return new DocIdSetIterator() {

          private int _docId = -1;

          @Override
          public int advance(int target) throws IOException {
            callAdvanceOnAllThatAreBehind(target);
            Arrays.sort(docIdSetIteratorArray, COMPARATOR);
            DocIdSetIterator iterator = docIdSetIteratorArray[0];
            return _docId = iterator.docID();
          }

          private void callAdvanceOnAllThatAreBehind(int target) throws IOException {
            for (int i = 0; i < docIdSetIteratorArray.length; i++) {
              DocIdSetIterator iterator = docIdSetIteratorArray[i];
              if (iterator.docID() < target) {
                iterator.advance(target);
              }
            }
          }

          @Override
          public int nextDoc() throws IOException {
            return advance(_docId + 1);
          }

          @Override
          public int docID() {
            return _docId;
          }

          @Override
          public long cost() {
            return cost;
          }

        };
      }
    };
  }

}