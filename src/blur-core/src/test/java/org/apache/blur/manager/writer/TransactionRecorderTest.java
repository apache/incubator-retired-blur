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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.blur.MiniCluster;
import org.apache.blur.index.IndexWriter;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransactionRecorderTest {
  
  @BeforeClass
  public static void setup() {
    MiniCluster.startDfs("target/transaction-recorder-test");
  }
  
  @AfterClass
  public static void teardown() {
    MiniCluster.shutdownDfs();
  }

  @Test
  public void testReplaySimpleTest() throws IOException, InterruptedException {
    Configuration configuration = new Configuration();
    URI fileSystemUri = MiniCluster.getFileSystemUri();
    Path path = new Path(fileSystemUri.toString() + "/transaction-recorder-test");
    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.delete(path, true);

    KeywordAnalyzer analyzer = new KeywordAnalyzer();

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("table");
    tableDescriptor.setTableUri(new Path(path, "tableuri").toUri().toString());
    tableDescriptor.setAnalyzerDefinition(new AnalyzerDefinition());

    TableContext tableContext = TableContext.create(tableDescriptor);
    ShardContext shardContext = ShardContext.create(tableContext, "shard-1");
    TransactionRecorder transactionRecorder = new TransactionRecorder(shardContext);
    transactionRecorder.open();
    try {
      transactionRecorder.replaceRow(true, genRow(), null);
      fail("Should NPE");
    } catch (NullPointerException e) {
    }
    
    Thread.sleep(TimeUnit.NANOSECONDS.toMillis(tableContext.getTimeBetweenWALSyncsNanos()) * 2);

    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, analyzer);
    IndexWriter writer = new IndexWriter(directory, conf);

    TransactionRecorder replayTransactionRecorder = new TransactionRecorder(shardContext);
    replayTransactionRecorder.replay(writer);
    IndexReader reader = DirectoryReader.open(directory);
    assertEquals(1, reader.numDocs());
    
    // cleanup recorder
    transactionRecorder.close();
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
