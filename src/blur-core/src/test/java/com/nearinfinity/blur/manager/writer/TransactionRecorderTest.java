package com.nearinfinity.blur.manager.writer;

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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.IndexWriter;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;

public class TransactionRecorderTest {

  @Test
  public void testReplay() throws IOException {
    String tmpPath = "./tmp/transaction-recorder/wal";
    rm(new File(tmpPath));

    KeywordAnalyzer analyzer = new KeywordAnalyzer();
    Configuration configuration = new Configuration();
    BlurAnalyzer blurAnalyzer = new BlurAnalyzer(analyzer);

    TransactionRecorder transactionRecorder = new TransactionRecorder();
    transactionRecorder.setAnalyzer(blurAnalyzer);
    transactionRecorder.setConfiguration(configuration);

    transactionRecorder.setWalPath(new Path(tmpPath));
    transactionRecorder.init();
    transactionRecorder.open();
    try {
      transactionRecorder.replaceRow(true, genRow(), null);
      fail("Should NPE");
    } catch (NullPointerException e) {
    }
    transactionRecorder.close(); // this is done so that the rawfs will flush
                                 // the file to disk for reading

    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter writer = new IndexWriter(directory, conf);

    TransactionRecorder replayTransactionRecorder = new TransactionRecorder();
    replayTransactionRecorder.setAnalyzer(blurAnalyzer);
    replayTransactionRecorder.setConfiguration(configuration);
    replayTransactionRecorder.setWalPath(new Path(tmpPath));
    replayTransactionRecorder.init();

    replayTransactionRecorder.replay(writer);
    IndexReader reader = IndexReader.open(directory);
    assertEquals(1, reader.numDocs());
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

  private Row genRow() {
    Row row = new Row();
    row.id = "1";
    Record record = new Record();
    record.recordId = "1";
    record.family = "test";
    record.addToColumns(new Column("name", "value"));
    row.addToRecords(record);
    return row;
  }

}
