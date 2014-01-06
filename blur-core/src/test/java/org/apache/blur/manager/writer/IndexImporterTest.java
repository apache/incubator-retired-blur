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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexImporterTest {

  private static final Path TMPDIR = new Path("target/tmp");

  private Path _base;
  private Configuration configuration;
  private IndexWriter _commitWriter;
  private IndexImporter _indexImporter;
  private Random _random = new Random();
  private Path _path;
  private Path _badRowIdsPath;
  private IndexWriter _mainWriter;
  private FileSystem _fileSystem;

  private FieldManager _fieldManager;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    configuration = new Configuration();
    _base = new Path(TMPDIR, "blur-index-importer-test");
    _fileSystem = _base.getFileSystem(configuration);
    _fileSystem.delete(_base, true);
    _fileSystem.mkdirs(_base);
    setupWriter(configuration);
  }

  private void setupWriter(Configuration configuration) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test-table");
    String uuid = UUID.randomUUID().toString();

    tableDescriptor.setTableUri(new Path(_base, "table-table").toUri().toString());
    tableDescriptor.setShardCount(2);

    TableContext tableContext = TableContext.create(tableDescriptor);
    ShardContext shardContext = ShardContext.create(tableContext, "shard-00000000");
    Path tablePath = new Path(_base, "table-table");
    Path shardPath = new Path(tablePath, "shard-00000000");
    String indexDirName = "index_" + uuid;
    _path = new Path(shardPath, indexDirName + ".commit");
    _fileSystem.mkdirs(_path);
    _badRowIdsPath = new Path(shardPath, indexDirName + ".bad_rowids");
    Directory commitDirectory = new HdfsDirectory(configuration, _path);
    Directory mainDirectory = new HdfsDirectory(configuration, shardPath);
    _fieldManager = tableContext.getFieldManager();
    Analyzer analyzerForIndex = _fieldManager.getAnalyzerForIndex();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, analyzerForIndex);
    conf.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);
    _commitWriter = new IndexWriter(commitDirectory, conf.clone());

    _mainWriter = new IndexWriter(mainDirectory, conf.clone());
    BufferStore.initNewBuffer(128, 128 * 128);

    _indexImporter = new IndexImporter(_mainWriter, new ReentrantReadWriteLock(), shardContext, TimeUnit.MINUTES, 10);
  }

  @After
  public void tearDown() throws IOException {
    IOUtils.closeQuietly(_mainWriter);
    IOUtils.closeQuietly(_indexImporter);
    _base.getFileSystem(configuration).delete(_base, true);
  }

  @Test
  public void testIndexImporterWithCorrectRowIdShardCombination() throws IOException {
    List<Field> document = _fieldManager.getFields("1", genRecord("1"));
    _commitWriter.addDocument(document);
    _commitWriter.commit();
    _commitWriter.close();
    _indexImporter.run();
    assertFalse(_fileSystem.exists(_path));
    assertFalse(_fileSystem.exists(_badRowIdsPath));
  }

  @Test
  public void testIndexImporterWithWrongRowIdShardCombination() throws IOException {
    setupWriter(configuration);
    List<Field> document = _fieldManager.getFields("2", genRecord("1"));
    _commitWriter.addDocument(document);
    _commitWriter.commit();
    _commitWriter.close();
    _indexImporter.run();
    assertFalse(_fileSystem.exists(_path));
    assertTrue(_fileSystem.exists(_badRowIdsPath));
  }

  private Record genRecord(String recordId) {
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(recordId);
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(_random.nextLong())));
    }
    return record;
  }

}
