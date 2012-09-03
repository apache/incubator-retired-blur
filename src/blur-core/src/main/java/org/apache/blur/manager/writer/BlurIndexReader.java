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
import java.io.IOException;

import org.apache.blur.index.IndexWriter;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;


public class BlurIndexReader extends AbstractBlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexReader.class);

  public void init() throws IOException {
    initIndexWriterConfig();
    Directory directory = getDirectory();
    if (!IndexReader.indexExists(directory)) {
      IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
      new IndexWriter(directory, conf).close();
    }
    initIndexReader(IndexReader.open(directory));
  }

  @Override
  public synchronized void refresh() throws IOException {
    // Override so that we can call within synchronized method
    super.refresh();
  }

  @Override
  public void close() throws IOException {
    super.close();
    LOG.info("Reader for table [{0}] shard [{1}] closed.", getTable(), getShard());
  }

  @Override
  public void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    // Do nothing
  }
}
