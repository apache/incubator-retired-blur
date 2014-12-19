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
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class TableShardCountCollapserTest {

  private static final int NUMBER_OF_BASE_SHARDS = 128;
  private Configuration configuration;
  private Path path;

  @Before
  public void setup() throws IOException {
    BufferStore.initNewBuffer(128, 128 * 128);
    configuration = new Configuration();
    path = new Path("./target/tmp-shards-for-testing");
    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.delete(path, true);
    createShards(NUMBER_OF_BASE_SHARDS);
  }

  private void createShards(int shardCount) throws IOException {
    for (int i = 0; i < shardCount; i++) {
      String shardName = ShardUtil.getShardName(i);
      createShard(configuration, i, new Path(path, shardName), shardCount);
    }
  }

  @Test
  public void testShardCountReducer() throws IOException {
    assertData(NUMBER_OF_BASE_SHARDS);
    TableShardCountCollapser t = new TableShardCountCollapser();
    t.setConf(configuration);
    t.setTablePath(path);
    int totalShardCount = 4;
    t.collapseShardsTo(totalShardCount);
    assertData(totalShardCount);
  }

  private void assertData(int totalShardCount) throws IOException {
    Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();
    for (int i = 0; i < totalShardCount; i++) {
      HdfsDirectory directory = new HdfsDirectory(configuration, new Path(path, ShardUtil.getShardName(i)));
      DirectoryReader reader = DirectoryReader.open(directory);
      int numDocs = reader.numDocs();
      for (int d = 0; d < numDocs; d++) {
        Document document = reader.document(d);
        IndexableField field = document.getField("id");
        Integer id = (Integer) field.numericValue();
        int partition = partitioner.getPartition(new IntWritable(id), null, totalShardCount);
        assertEquals(i, partition);
      }
      reader.close();
    }
  }

  private static void createShard(Configuration configuration, int i, Path path, int totalShardCount)
      throws IOException {
    HdfsDirectory hdfsDirectory = new HdfsDirectory(configuration, path);
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter indexWriter = new IndexWriter(hdfsDirectory, conf);

    Partitioner<IntWritable, IntWritable> partitioner = new HashPartitioner<IntWritable, IntWritable>();
    int partition = partitioner.getPartition(new IntWritable(i), null, totalShardCount);
    assertEquals(i, partition);

    Document doc = getDoc(i);
    indexWriter.addDocument(doc);
    indexWriter.close();
  }

  private static Document getDoc(int i) {
    Document document = new Document();
    document.add(new IntField("id", i, Store.YES));
    return document;
  }

}
