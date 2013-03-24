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

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

public class TermDocIterableTest {

  private static final int BLOCKS = 10;
  private static final int COUNT_PER_BLOCK = 100;
  private AtomicReader reader;

  @Before
  public void setup() throws IOException {
    reader = createIndexReader();
  }

  @Test
  public void testTermDocIterable() throws IOException {
    for (int pass = 0; pass < 1; pass++) {
      for (int id = 0; id < BLOCKS; id++) {
        DocsEnum termDocs = reader.termDocsEnum(new Term("id", Integer.toString(id)));
        TermDocIterable iterable = new TermDocIterable(termDocs, reader);
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

  private AtomicReader createIndexReader() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(LUCENE_VERSION, new StandardAnalyzer(LUCENE_VERSION)));
    for (int i = 0; i < BLOCKS; i++) {
      addDocumentBlock(i, COUNT_PER_BLOCK, writer);
    }
    writer.close();
    return SlowCompositeReaderWrapper.wrap(DirectoryReader.open(directory));
  }

  private void addDocumentBlock(int id, int count, IndexWriter writer) throws IOException {
    FieldType fieldType = new FieldType();
    fieldType.setIndexed(true);
    fieldType.setOmitNorms(true);
    fieldType.setTokenized(false);
    fieldType.setStored(true);

    FieldType fieldTypeNoIndex = new FieldType();
    fieldTypeNoIndex.setStored(true);
    fieldTypeNoIndex.setIndexed(false);

    for (int i = 0; i < count; i++) {
      Document document = new Document();
      document.add(new Field("id", Integer.toString(id), fieldType));
      document.add(new Field("field", Integer.toString(i), fieldType));
      for (int j = 0; j < 100; j++) {
        document.add(new Field("field" + j, "testing here testing here testing here testing here testing here testing here testing here", fieldTypeNoIndex));
      }
      writer.addDocument(document);
    }
  }

}
