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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlurIndexReaderTest {

  private static final File TMPDIR = new File("./target/tmp/BlurIndexReaderTest");

  private ExecutorService service;
  private File base;
  private Configuration configuration;

  private DirectoryReferenceFileGC gc;
  private SharedMergeScheduler mergeScheduler;
  private BlurIndexReader reader;

  private BlurIndexRefresher refresher;
  private BlurIndexCloser indexCloser;
  private FSDirectory directory;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    base = new File(TMPDIR, "blur-index-reader-test");
    rm(base);
    base.mkdirs();

    mergeScheduler = new SharedMergeScheduler();
    gc = new DirectoryReferenceFileGC();

    configuration = new Configuration();
    service = Executors.newThreadPool("test", 1);

  }

  private void setupWriter(Configuration configuration, long refresh) throws IOException, URISyntaxException {
    String tableUri = new File(base, "table-store-" + UUID.randomUUID().toString()).toURI().toString();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test-table");
    tableDescriptor.setTableUri(tableUri);
    tableDescriptor.putToTableProperties("blur.shard.time.between.refreshs", Long.toString(refresh));
    tableDescriptor.putToTableProperties("blur.shard.time.between.commits", Long.toString(1000));

    TableContext tableContext = TableContext.create(tableDescriptor);
    directory = FSDirectory.open(new File(new URI(tableDescriptor.getTableUri())));

    ShardContext shardContext = ShardContext.create(tableContext, "test-shard");
    refresher = new BlurIndexRefresher();
    indexCloser = new BlurIndexCloser();
    reader = new BlurIndexReader(shardContext, directory, refresher, indexCloser);
  }

  @After
  public void tearDown() throws IOException {
    reader.close();
    mergeScheduler.close();
    gc.close();
    service.shutdownNow();
    refresher.close();
    indexCloser.close();
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
  public void testBlurIndexWriter() throws IOException, InterruptedException, URISyntaxException {
    setupWriter(configuration, 1);
    IndexSearcherClosable indexReader1 = reader.getIndexReader();
    doWrite();
    assertEquals(0, indexReader1.getIndexReader().numDocs());
    indexReader1.close();
    reader.refresh();
    IndexSearcherClosable indexReader2 = reader.getIndexReader();
    assertEquals(1, indexReader2.getIndexReader().numDocs());
    indexReader2.close();
  }

  private void doWrite() throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    BlurIndexWriter writer = new BlurIndexWriter(directory, conf);
    writer.addDocument(getDoc());
    writer.close();
  }

  private Document getDoc() {
    return new Document();
  }

}
