package org.apache.blur.utils;

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
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

public class TermDocIterable implements Iterable<Document> {

  private DocsEnum docsEnum;
  private AtomicReader reader;
  private ResetableDocumentStoredFieldVisitor fieldSelector;

  public TermDocIterable(DocsEnum docsEnum, AtomicReader reader) {
    this(docsEnum, reader, new ResetableDocumentStoredFieldVisitor());
  }

  public TermDocIterable(DocsEnum docsEnum, AtomicReader reader, ResetableDocumentStoredFieldVisitor fieldSelector) {
    if (docsEnum == null) {
      throw new NullPointerException("docsEnum can not be null.");
    }
    this.docsEnum = docsEnum;
    this.reader = reader;
    this.fieldSelector = fieldSelector;
  }

  @Override
  public Iterator<Document> iterator() {
    return new Iterator<Document>() {
      private boolean hasNext = getNext();

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public Document next() {
        Document doc;
        try {
          doc = getDoc();
          hasNext = getNext();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return doc;
      }

      @Override
      public void remove() {

      }
    };
  }

  private Document getDoc() throws IOException {
    fieldSelector.reset();
    reader.document(docsEnum.docID(), fieldSelector);
    return fieldSelector.getDocument();
  }

  private boolean getNext() {
    try {
      int next = docsEnum.nextDoc();
      if (next == DocIdSetIterator.NO_MORE_DOCS) {
        return false;
      }
      Bits liveDocs = MultiFields.getLiveDocs(reader);
      if (liveDocs != null) {
        while (!liveDocs.get(docsEnum.docID())) {
          next = docsEnum.nextDoc();
        }
      }
      return next == DocIdSetIterator.NO_MORE_DOCS ? false : true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
