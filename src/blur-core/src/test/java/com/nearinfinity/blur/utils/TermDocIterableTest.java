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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class TermDocIterableTest {

  private static final int BLOCKS = 10;
  private static final int COUNT_PER_BLOCK = 100;
  private IndexReader reader;

  @Before
  public void setup() throws IOException {
    reader = createIndexReader();
  }

  @SuppressWarnings("serial")
  @Test
  public void testTermDocIterable() throws IOException {
    for (int pass = 0; pass < 1; pass++) {
      for (int id = 0; id < BLOCKS; id++) {
        TermDocs termDocs = reader.termDocs(new Term("id", Integer.toString(id)));
        TermDocIterable iterable = new TermDocIterable(termDocs, reader, new FieldSelector() {
          @Override
          public FieldSelectorResult accept(String fieldName) {
            return FieldSelectorResult.LOAD;
          }
        });
        int count = 0;
        int i = 0;
        long s = System.nanoTime();
        for (Document document : iterable) {
          count++;
          assertEquals(i, Integer.parseInt(document.get("field")));
          i++;
        }
        long time = System.nanoTime() - s;
        System.out.println(time / 1000000.0 + " " + id + " " + pass);
        assertEquals(COUNT_PER_BLOCK, count);
      }
    }
  }

  private IndexReader createIndexReader() throws IOException {
    FSDirectory directory = FSDirectory.open(new File("./tmp/termdociterable"));
    if (!IndexReader.indexExists(directory)) {
      rm(new File("./tmp/termdociterable"));
      IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(BlurConstants.LUCENE_VERSION, new StandardAnalyzer(BlurConstants.LUCENE_VERSION)));
      for (int i = 0; i < BLOCKS; i++) {
        addDocumentBlock(i, COUNT_PER_BLOCK, writer);
      }
      writer.close();
    }
    return IndexReader.open(directory);
  }

  private File rm(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
    return file;
  }

  private void addDocumentBlock(int id, int count, IndexWriter writer) throws IOException {
    for (int i = 0; i < count; i++) {
      Document document = new Document();
      document.add(new Field("id", Integer.toString(id), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
      document.add(new Field("field", Integer.toString(i), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
      for (int j = 0; j < 100; j++) {
        document.add(new Field("field" + j, "testing here testing here testing here testing here testing here testing here testing here", Store.YES, Index.NO));
      }
      writer.addDocument(document);
    }
  }

}
