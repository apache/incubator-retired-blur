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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.lucene.search.FairSimilarity;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class BlurNRTIndexTest {

  private static final int TEST_NUMBER_WAIT_VISIBLE = 500;
  private static final int TEST_NUMBER = 50000;
  private BlurNRTIndex writer;
  private BlurIndexCloser closer;
  private Random random = new Random();
  private BlurIndexRefresher refresher;
  private ExecutorService service;
  private File base;

  @Before
  public void setup() throws IOException {
    base = new File("./tmp/blur-index-writer-test");
    rm(base);
    base.mkdirs();
    closer = new BlurIndexCloser();
    closer.init();

    Configuration configuration = new Configuration();

    BlurAnalyzer analyzer = new BlurAnalyzer(new KeywordAnalyzer());

    refresher = new BlurIndexRefresher();
    refresher.init();

    writer = new BlurNRTIndex();
    writer.setDirectory(FSDirectory.open(new File(base, "index")));
    writer.setCloser(closer);
    writer.setAnalyzer(analyzer);
    writer.setSimilarity(new FairSimilarity());
    writer.setTable("testing-table");
    writer.setShard("testing-shard");

    service = Executors.newThreadPool("test", 10);
    writer.setWalPath(new Path(new File(base, "wal").toURI()));

    writer.setConfiguration(configuration);
    writer.setTimeBetweenRefreshs(25);
    writer.init();
  }

  @After
  public void tearDown() throws IOException {
    refresher.close();
    writer.close();
    closer.close();
    service.shutdownNow();
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
  public void testBlurIndexWriter() throws IOException {
    long s = System.nanoTime();
    int total = 0;
    for (int i = 0; i < TEST_NUMBER_WAIT_VISIBLE; i++) {
      writer.replaceRow(true, true, genRow());
      IndexReader reader = writer.getIndexReader();
      assertEquals(i + 1, reader.numDocs());
      total++;
    }
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    IndexReader reader = writer.getIndexReader();
    assertEquals(TEST_NUMBER_WAIT_VISIBLE, reader.numDocs());
  }

  @Test
  public void testBlurIndexWriterFaster() throws IOException, InterruptedException {
    long s = System.nanoTime();
    int total = 0;
    for (int i = 0; i < TEST_NUMBER; i++) {
      writer.replaceRow(false, true, genRow());
      total++;
    }
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    writer.refresh();
    IndexReader reader = writer.getIndexReader();
    assertEquals(TEST_NUMBER, reader.numDocs());
  }

  private Row genRow() {
    Row row = new Row();
    row.setId(Long.toString(random.nextLong()));
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(Long.toString(random.nextLong()));
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(random.nextLong())));
    }
    row.addToRecords(record);
    return row;
  }

}
