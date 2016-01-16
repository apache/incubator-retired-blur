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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IndexSearcherCloseableBase;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexImporterTest {

  private static final Path TMPDIR = new Path("target/tmp");

  private Path _base;
  private Configuration _configuration;
  private IndexWriter _commitWriter;
  private IndexImporter _indexImporter;
  private Random _random = new Random();
  private Path _path;
  private Path _badRowIdsPath;
  private IndexWriter _mainWriter;
  private FileSystem _fileSystem;
  private FieldManager _fieldManager;
  private Path _badIndexPath;
  private Path _inUsePath;
  private Path _shardPath;
  private HdfsDirectory _mainDirectory;
  private Timer _timer;

  @Before
  public void setup() throws IOException {
    _timer = new Timer("Index Importer", true);
    TableContext.clear();
    _configuration = new Configuration();
    _base = new Path(TMPDIR, "blur-index-importer-test");
    _fileSystem = _base.getFileSystem(_configuration);
    _fileSystem.delete(_base, true);
    _fileSystem.mkdirs(_base);
    setupWriter(_configuration);
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
    _shardPath = new Path(tablePath, "shard-00000000");
    String indexDirName = "index_" + uuid;
    _path = new Path(_shardPath, indexDirName + ".commit");
    _fileSystem.mkdirs(_path);
    _badRowIdsPath = new Path(_shardPath, indexDirName + ".badrowids");
    _badIndexPath = new Path(_shardPath, indexDirName + ".badindex");
    _inUsePath = new Path(_shardPath, indexDirName + ".inuse");
    Directory commitDirectory = new HdfsDirectory(configuration, _path);
    _mainDirectory = new HdfsDirectory(configuration, _shardPath);
    _fieldManager = tableContext.getFieldManager();
    Analyzer analyzerForIndex = _fieldManager.getAnalyzerForIndex();
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, analyzerForIndex);
    // conf.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    _commitWriter = new IndexWriter(commitDirectory, conf.clone());

    // Make sure there's an empty index...
    new IndexWriter(_mainDirectory, conf.clone()).close();
    _mainWriter = new IndexWriter(_mainDirectory, conf.clone());
    BufferStore.initNewBuffer(128, 128 * 128);

    _indexImporter = new IndexImporter(_timer, getBlurIndex(shardContext, _mainDirectory), shardContext,
        TimeUnit.MINUTES, 10, 10, null, _mainDirectory);
  }

  private BlurIndex getBlurIndex(ShardContext shardContext, final Directory mainDirectory) throws IOException {
    return new BlurIndex(new BlurIndexConfig(shardContext, mainDirectory, null, null, null, null, null, null, null, 0)) {

      @Override
      public void removeSnapshot(String name) throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void process(IndexAction indexAction) throws IOException {
        final DirectoryReader reader = DirectoryReader.open(mainDirectory);
        IndexSearcherCloseable searcherClosable = new IndexSearcherCloseableBase(reader, null) {

          @Override
          public Directory getDirectory() {
            return mainDirectory;
          }

          @Override
          public void close() throws IOException {

          }
        };
        try {
          indexAction.performMutate(searcherClosable, _mainWriter);
          indexAction.doPreCommit(searcherClosable, _mainWriter);
          _mainWriter.commit();
          indexAction.doPostCommit(_mainWriter);
        } catch (IOException e) {
          indexAction.doPreRollback(_mainWriter);
          _mainWriter.rollback();
          indexAction.doPostRollback(_mainWriter);
        }
      }

      @Override
      public void optimize(int numberOfSegmentsPerShard) throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public AtomicBoolean isClosed() {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public List<String> getSnapshots() throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public IndexSearcherCloseable getIndexSearcher() throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void createSnapshot(String name) throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void close() throws IOException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void enqueue(List<RowMutation> mutations) {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void finishBulkMutate(String bulkId, boolean apply, boolean blockUntilComplete) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void addBulkMutate(String bulkId, RowMutation mutation) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportPendingCount() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportInProgressCount() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getOnDiskSize() throws IOException {
        throw new RuntimeException("Not Implemented");
      }
    };
  }

  @After
  public void tearDown() throws IOException {
    _timer.cancel();
    _timer.purge();
    IOUtils.closeQuietly(_mainWriter);
    IOUtils.closeQuietly(_indexImporter);
    _base.getFileSystem(_configuration).delete(_base, true);
  }

  @Test
  public void testIndexImporterWithBadIndex() throws IOException {
    _commitWriter.close();
    _fileSystem.delete(_path, true);
    _fileSystem.mkdirs(_path);
    _indexImporter.run();
    assertFalse(_fileSystem.exists(_path));
    assertFalse(_fileSystem.exists(_badRowIdsPath));
    assertTrue(_fileSystem.exists(_badIndexPath));
    validateIndex();
  }

  private void validateIndex() throws IOException {
    HdfsDirectory dir = new HdfsDirectory(_configuration, _shardPath);
    DirectoryReader.open(dir).close();
  }

  @Test
  public void testIndexImporterCheckInuseDirsForCleanup() throws IOException {
    FileSystem fileSystem = _shardPath.getFileSystem(_configuration);

    List<Field> document1 = _fieldManager.getFields("10", genRecord("1"));
    _mainWriter.addDocument(document1);
    _mainWriter.commit();

    List<Field> document2 = _fieldManager.getFields("1", genRecord("1"));
    _commitWriter.addDocument(document2);
    _commitWriter.commit();
    _commitWriter.close();
    _indexImporter.run();

    assertFalse(_fileSystem.exists(_path));
    assertFalse(_fileSystem.exists(_badRowIdsPath));
    assertTrue(_fileSystem.exists(_inUsePath));

    DirectoryReader reader = DirectoryReader.open(_mainDirectory);
    while (reader.leaves().size() > 1) {
      reader.close();
      _mainWriter.forceMerge(1, true);
      _mainWriter.commit();
      reader = DirectoryReader.open(_mainDirectory);
    }
    _indexImporter.cleanupOldDirs();
    assertFalse(fileSystem.exists(_path));
    validateIndex();
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
    assertTrue(_fileSystem.exists(_inUsePath));
    validateIndex();
  }

  @Test
  public void testIndexImporterWithWrongRowIdShardCombination() throws IOException {
    List<Field> document = _fieldManager.getFields("2", genRecord("1"));
    _commitWriter.addDocument(document);
    _commitWriter.commit();
    _commitWriter.close();
    _indexImporter.run();
    assertFalse(_fileSystem.exists(_path));
    assertTrue(_fileSystem.exists(_badRowIdsPath));
    assertFalse(_fileSystem.exists(_inUsePath));
    validateIndex();
  }

  @Test
  public void testIndexImporterWhenThereIsAFailureOnDuringImport() throws IOException {
    List<Field> document = _fieldManager.getFields("1", genRecord("1"));
    _commitWriter.addDocument(document);
    _commitWriter.commit();
    _commitWriter.close();
    _indexImporter.setTestError(new Runnable() {
      @Override
      public void run() {
        throw new RuntimeException("test");
      }
    });
    try {
      _indexImporter.run();
    } catch (RuntimeException e) {
      assertEquals("test", e.getMessage());
    }
    _indexImporter.cleanupOldDirs();
    _indexImporter.setTestError(null);
    _indexImporter.run();
    assertFalse(_fileSystem.exists(_path));
    assertFalse(_fileSystem.exists(_badRowIdsPath));
    assertTrue(_fileSystem.exists(_inUsePath));
    validateIndex();
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
