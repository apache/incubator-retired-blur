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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IndexSearcherCloseableBase;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class MutatableActionTest {

  private static final File TMPDIR = new File("./target/tmp");
  private static final String TABLE = "test";

  private MutatableAction _action;
  private final Random _random = new Random();
  private IndexWriterConfig _conf;
  private File _base;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    _base = new File(TMPDIR, "MutatableActionTest");
    rmr(_base);

    File file = new File(_base, TABLE);
    file.mkdirs();

    TableContext.clear();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setTableUri(file.toURI().toString());
    TableContext tableContext = TableContext.create(tableDescriptor);
    ShardContext shardContext = ShardContext.create(tableContext, "test");
    _action = new MutatableAction(shardContext);
    _conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @Test
  public void testReplaceRow() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(1, reader.numDocs());

    Row row2 = new Row(row);
    List<Column> cols = new ArrayList<Column>();
    cols.add(new Column("n", "v"));
    row2.addToRecords(new Record("1", "fam", cols));

    _action.replaceRow(row2);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());
  }

  private IndexSearcherCloseable getSearcher(DirectoryReader reader, final Directory directory) {
    return new IndexSearcherCloseableBase(reader, null) {

      @Override
      public Directory getDirectory() {
        return directory;
      }

      @Override
      public void close() throws IOException {

      }
    };
  }

  @Test
  public void testDeleteRecord() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    List<Column> cols = new ArrayList<Column>();
    cols.add(new Column("n", "v"));
    row.addToRecords(new Record("1", "fam", cols));

    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    _action.deleteRecord(row.getId(), "1");
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(1, reader.numDocs());
  }

  @Test
  public void testDeleteRow() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(1, reader.numDocs());

    _action.deleteRow(row.getId());
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(0, reader.numDocs());
  }

  @Test
  public void testReplaceRecord() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    List<Column> cols = new ArrayList<Column>();
    cols.add(new Column("n", "v"));
    row.addToRecords(new Record("1", "fam", cols));

    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    cols.add(new Column("n2", "v2"));
    Record record = new Record("1", "fam", cols);
    _action.replaceRecord(row.getId(), record);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term(BlurConstants.ROW_ID, row.getId())), 10);
    Document doc2 = searcher.doc(topDocs.scoreDocs[1].doc);
    List<IndexableField> fields = doc2.getFields();
    assertEquals(fields.size(), 5);
    String value = doc2.get("fam.n2");
    assertEquals("v2", value);
  }

  @Test
  public void testAppendColumns() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    List<Column> cols = new ArrayList<Column>();
    cols.add(new Column("n", "v"));
    row.addToRecords(new Record("1", "fam", cols));

    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    cols.clear();
    cols.add(new Column("n2", "v2"));
    Record record = new Record("1", "fam", cols);
    _action.appendColumns(row.getId(), record);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term(BlurConstants.ROW_ID, row.getId())), 10);
    Document doc2 = searcher.doc(topDocs.scoreDocs[1].doc);
    List<IndexableField> fields = doc2.getFields();
    assertEquals(fields.size(), 5);
    String value = doc2.get("fam.n2");
    assertEquals("v2", value);
  }

  @Test
  public void testReplaceColumns() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    DirectoryReader reader = getIndexReader(directory);
    IndexWriter writer = new IndexWriter(directory, _conf.clone());
    assertEquals(0, reader.numDocs());

    Row row = genRow();
    List<Column> cols = new ArrayList<Column>();
    cols.add(new Column("n", "v"));
    cols.add(new Column("n1", "v1"));
    row.addToRecords(new Record("1", "fam", cols));

    _action.replaceRow(row);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    cols.clear();
    cols.add(new Column("n1", "v2"));
    Record record = new Record("1", "fam", cols);
    _action.replaceColumns(row.getId(), record);
    _action.performMutate(getSearcher(reader, directory), writer);
    reader = commitAndReopen(reader, writer);
    assertEquals(2, reader.numDocs());

    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term(BlurConstants.ROW_ID, row.getId())), 10);
    Document doc2 = searcher.doc(topDocs.scoreDocs[1].doc);
    List<IndexableField> fields = doc2.getFields();
    assertEquals(5, fields.size());
    String value = doc2.get("fam.n1");
    assertEquals("v2", value);
  }

  private DirectoryReader commitAndReopen(DirectoryReader reader, IndexWriter writer) throws IOException {
    writer.commit();
    DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
    if (newReader == null) {
      throw new IOException("Should have new data.");
    }
    reader.close();
    return newReader;
  }

  private DirectoryReader getIndexReader(RAMDirectory directory) throws IOException {
    if (!DirectoryReader.indexExists(directory)) {
      new IndexWriter(directory, _conf.clone()).close();
    }
    return DirectoryReader.open(directory);
  }

  private Row genRow() {
    Row row = new Row();
    row.setId(Long.toString(_random.nextLong()));
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(Long.toString(_random.nextLong()));
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(_random.nextLong())));
    }
    row.addToRecords(record);
    return row;
  }
}
