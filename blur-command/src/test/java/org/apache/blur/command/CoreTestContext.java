package org.apache.blur.command;

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

import org.apache.blur.BlurConfiguration;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IndexSearcherCloseableBase;
import org.apache.blur.server.TableContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class CoreTestContext extends IndexContext {
  private RAMDirectory directory = new RAMDirectory();

  private CoreTestContext() {
  }

  /**
   * Index will contain 26 documents with the following column/values: alpha =
   * double-letter a-z (lowercase characters); num = 0-25 val = val (constant
   * across all docs)
   * 
   * New columns may be added so don't rely on the column count in tests.
   * 
   * @return
   */
  public static IndexContext newSimpleAlpaNumContext() {
    CoreTestContext ctx = new CoreTestContext();

    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new StandardAnalyzer(Version.LUCENE_43));
    try {
      IndexWriter writer = new IndexWriter(ctx.directory, conf);

      for (int i = 0; i < 26; i++) {
        String alpha = new Character((char) (97 + i)).toString();
        Document doc = new Document();

        doc.add(new Field("id", Integer.toString(i), TextField.TYPE_STORED));
        doc.add(new Field("alpha", alpha + alpha, TextField.TYPE_STORED));
        doc.add(new Field("num", Integer.toString(i), TextField.TYPE_STORED));
        doc.add(new Field("val", "val", TextField.TYPE_STORED));

        writer.addDocument(doc);

        writer.commit();
      }
      writer.commit();
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create test context.", e);
    }

    return ctx;
  }

  @Override
  public TableContext getTableContext() {
    return null;
  }

  @Override
  public Shard getShard() {
    return null;
  }

  @Override
  public IndexSearcherCloseable getIndexSearcher() {
    return new IndexSearcherCloseableBase(getIndexReader(), null) {

      @Override
      public Directory getDirectory() {
        return directory;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  @Override
  public IndexReader getIndexReader() {
    try {
      return DirectoryReader.open(directory);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public BlurConfiguration getBlurConfiguration() {
    return null;
  }

  @Override
  public TableContext getTableContext(String table) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public BlurConfiguration getBlurConfiguration(String table) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

}