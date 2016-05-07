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
package org.apache.blur.mapreduce.lib;

import java.io.IOException;

import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ParallelAtomicReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

public class PrimeDocOverFlowHelper {

  private static Directory _directoryNewRow;

  static {
    try {
      _directoryNewRow = new RAMDirectory();
      IndexWriter writer = new IndexWriter(_directoryNewRow, new IndexWriterConfig(Version.LUCENE_43,
          new KeywordAnalyzer()));
      Document document = new Document();
      document.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      document.add(new StringField(BlurConstants.NEW_ROW, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      writer.addDocument(document);
      writer.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public static AtomicReader addPrimeDoc(AtomicReader atomicReader, boolean newRow, String currentRowId)
      throws IOException {
    AtomicReaderContext context = DirectoryReader.open(newRow ? _directoryNewRow : getDirectoryUpdateRow(currentRowId))
        .leaves().get(0);
    return new ParallelAtomicReader(true, setDocSize(context.reader(), atomicReader.maxDoc()), atomicReader);
  }

  private static Directory getDirectoryUpdateRow(String currentRowId) {
    try {
      RAMDirectory directoryUpdateRow = new RAMDirectory();
      IndexWriter writer = new IndexWriter(directoryUpdateRow, new IndexWriterConfig(Version.LUCENE_43,
          new KeywordAnalyzer()));
      Document document = new Document();
      document.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
      document.add(new StringField(BlurConstants.UPDATE_ROW, currentRowId, Store.NO));
      writer.addDocument(document);
      writer.close();
      return directoryUpdateRow;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static AtomicReader setDocSize(AtomicReader reader, final int count) {
    return new FilterAtomicReader(reader) {
      @Override
      public Bits getLiveDocs() {
        return new Bits() {
          @Override
          public boolean get(int index) {
            return true;
          }

          @Override
          public int length() {
            return count;
          }
        };
      }

      @Override
      public int numDocs() {
        return count;
      }

      @Override
      public int maxDoc() {
        return count;
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        // Do nothing
      }
    };
  }
}
