package org.apache.blur.manager.writer;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.index.IndexWriter;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexImporterTest {

  private static final File TMPDIR = new File("./target/tmp");
  
  private File base;
  private Configuration configuration;
  private IndexWriter writer;
  private IndexImporter indexImporter;
  private Random random = new Random();
  private File path;
  private File badRowIdsPath;
  
  @Before
  public void setup() throws IOException {
    base = new File(TMPDIR, "blur-index-importer-test");
    rm(base);
    base.mkdirs();
    configuration = new Configuration();
  }

  private void setupWriter(Configuration configuration) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test-table");
    String uuid = UUID.randomUUID().toString();
    tableDescriptor.setTableUri(new File(base, "table-store").toURI().toString());
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());
    tableDescriptor.setShardCount(2);
    TableContext tableContext = TableContext.create(tableDescriptor);
    ShardContext shardContext = ShardContext.create(tableContext, "shard-00000000");
    File tablePath = new File(base, "table-store");
    File shardPath = new File(tablePath, "shard-00000000");
    String indexDirName = "index_" + uuid;
    path = new File(shardPath, indexDirName +".commit");
    path.mkdirs();
    badRowIdsPath = new File(shardPath, indexDirName +".bad_rowids");
    FSDirectory directory = FSDirectory.open(path);
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, tableContext.getAnalyzer());
    writer = new IndexWriter(directory, conf);
    BufferStore.init(128, 128);
    indexImporter = new IndexImporter(new TrackingIndexWriter(writer), new ReentrantReadWriteLock(), shardContext, TimeUnit.MINUTES, 10);
  }

  @After
  public void tearDown() throws IOException {
    writer.close();
    indexImporter.close();
    rm(base);
  }

  private void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }
  
  @Test
  public void testIndexImporterWithCorrectRowIdShardCombination() throws IOException {
    setupWriter(configuration);
    Document document = TransactionRecorder.convert("1", genRecord("1"), new StringBuilder(), new BlurAnalyzer());
    writer.addDocument(document);
    writer.commit();
    indexImporter.run();
    assertFalse(path.exists());
    assertFalse(badRowIdsPath.exists());
  }
  
  @Test
  public void testIndexImporterWithWrongRowIdShardCombination() throws IOException {
    setupWriter(configuration);
    Document document = TransactionRecorder.convert("2", genRecord("1"), new StringBuilder(), new BlurAnalyzer());
    writer.addDocument(document);
    writer.commit();
    indexImporter.run();
    assertFalse(path.exists());
    assertTrue(badRowIdsPath.exists());
  }
  
  private Record genRecord(String recordId) {
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(recordId);
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(random.nextLong())));
    }
    return record;
  }

}
