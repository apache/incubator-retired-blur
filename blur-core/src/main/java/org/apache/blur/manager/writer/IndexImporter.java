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
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class IndexImporter extends TimerTask implements Closeable {
  private final static Log LOG = LogFactory.getLog(IndexImporter.class);

  private final BlurIndex _blurIndex;
  private final ShardContext _shardContext;
  private final Timer _timer;
  private final String _table;
  private final String _shard;

  public IndexImporter(BlurIndex blurIndex, ShardContext shardContext, TimeUnit refreshUnit, long refreshAmount) {
    _blurIndex = blurIndex;
    _shardContext = shardContext;
    _timer = new Timer("IndexImporter [" + shardContext.getShard() + "/" + shardContext.getTableContext().getTable()
        + "]", true);
    long period = refreshUnit.toMillis(refreshAmount);
    _timer.schedule(this, period, period);
    _table = _shardContext.getTableContext().getTable();
    _shard = _shardContext.getShard();
  }

  @Override
  public void close() throws IOException {
    _timer.cancel();
    _timer.purge();
  }

  @Override
  public void run() {
    Path path = _shardContext.getHdfsDirPath();
    Configuration configuration = _shardContext.getTableContext().getConfiguration();
    try {
      FileSystem fileSystem = path.getFileSystem(configuration);
      SortedSet<FileStatus> listStatus;
      while (true) {
        try {
          listStatus = sort(fileSystem.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
              if (path != null && path.getName().endsWith(".commit")) {
                return true;
              }
              return false;
            }
          }));
          break;
        } catch (FileNotFoundException e) {
          LOG.warn("File not found error, retrying.");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }
      List<HdfsDirectory> indexesToImport = new ArrayList<HdfsDirectory>();
      for (FileStatus fileStatus : listStatus) {
        Path file = fileStatus.getPath();
        if (fileStatus.isDir() && file.getName().endsWith(".commit")) {
          HdfsDirectory hdfsDirectory = new HdfsDirectory(configuration, file);
          if (!DirectoryReader.indexExists(hdfsDirectory)) {
            LOG.error("Directory found at [{0}] is not a vaild index.", file);
          } else {
            indexesToImport.add(hdfsDirectory);
          }
        }
      }
      if (indexesToImport.isEmpty()) {
        return;
      }

      IndexAction indexAction = getIndexAction(indexesToImport, fileSystem);
      _blurIndex.process(indexAction);
    } catch (IOException e) {
      LOG.error("Unknown error while trying to refresh imports on [{1}/{2}].", e, _shard, _table);
    }
  }

  private IndexAction getIndexAction(final List<HdfsDirectory> indexesToImport, final FileSystem fileSystem) {
    return new IndexAction() {

      private Path _dirPath;

      @Override
      public void performMutate(IndexSearcherClosable searcher, IndexWriter writer) throws IOException {
        for (Directory directory : indexesToImport) {
          LOG.info("About to import [{0}] into [{1}/{2}]", directory, _shard, _table);
        }
        LOG.info("Obtaining lock on [{0}/{1}]", _shard, _table);
        for (HdfsDirectory directory : indexesToImport) {
          boolean emitDeletes = searcher.getIndexReader().numDocs() != 0;
          _dirPath = directory.getPath();
          applyDeletes(directory, writer, _shard, emitDeletes);
          LOG.info("Add index [{0}] [{1}/{2}]", directory, _shard, _table);
          writer.addIndexes(directory);
          LOG.info("Removing delete markers [{0}] on [{1}/{2}]", directory, _shard, _table);
          writer.deleteDocuments(new Term(BlurConstants.DELETE_MARKER, BlurConstants.DELETE_MARKER_VALUE));
          LOG.info("Finishing import [{0}], commiting on [{1}/{2}]", directory, _shard, _table);
        }
      }

      @Override
      public void doPreCommit(IndexSearcherClosable indexSearcher, IndexWriter writer) throws IOException {

      }

      @Override
      public void doPostCommit(IndexWriter writer) throws IOException {
        LOG.info("Calling maybeMerge on the index [{0}] for [{1}/{2}]", _dirPath, _shard, _table);
        writer.maybeMerge();
        LOG.info("Cleaning up old directory [{0}] for [{1}/{2}]", _dirPath, _shard, _table);
        fileSystem.delete(_dirPath, true);
        LOG.info("Import complete on [{0}/{1}]", _shard, _table);
      }

      @Override
      public void doPreRollback(IndexWriter writer) throws IOException {
        LOG.info("Starting rollback on [{0}/{1}]", _shard, _table);
      }

      @Override
      public void doPostRollback(IndexWriter writer) throws IOException {
        LOG.info("Finished rollback on [{0}/{1}]", _shard, _table);
        String name = _dirPath.getName();
        int lastIndexOf = name.lastIndexOf('.');
        String badRowIdsName = name.substring(0, lastIndexOf) + ".bad_rowids";
        fileSystem.rename(_dirPath, new Path(_dirPath.getParent(), badRowIdsName));
      }
    };
  }

  private SortedSet<FileStatus> sort(FileStatus[] listStatus) {
    SortedSet<FileStatus> result = new TreeSet<FileStatus>();
    for (FileStatus fileStatus : listStatus) {
      result.add(fileStatus);
    }
    return result;
  }

  private void applyDeletes(Directory directory, IndexWriter indexWriter, String shard, boolean emitDeletes)
      throws IOException {
    DirectoryReader reader = DirectoryReader.open(directory);
    try {
      LOG.info("Applying deletes in reader [{0}]", reader);
      CompositeReaderContext compositeReaderContext = reader.getContext();
      List<AtomicReaderContext> leaves = compositeReaderContext.leaves();
      BlurPartitioner blurPartitioner = new BlurPartitioner();
      Text key = new Text();
      int numberOfShards = _shardContext.getTableContext().getDescriptor().getShardCount();
      int shardId = BlurUtil.getShardIndex(shard);
      for (AtomicReaderContext context : leaves) {
        AtomicReader atomicReader = context.reader();
        Fields fields = atomicReader.fields();
        Terms terms = fields.terms(BlurConstants.ROW_ID);
        if (terms != null) {
          TermsEnum termsEnum = terms.iterator(null);
          BytesRef ref = null;
          while ((ref = termsEnum.next()) != null) {
            key.set(ref.bytes, ref.offset, ref.length);
            int partition = blurPartitioner.getPartition(key, null, numberOfShards);
            if (shardId != partition) {
              throw new IOException("Index is corrupted, RowIds are found in wrong shard, partition [" + partition
                  + "] does not shard [" + shardId + "], this can happen when rows are not hashed correctly.");
            }
            if (emitDeletes) {
              indexWriter.deleteDocuments(new Term(BlurConstants.ROW_ID, BytesRef.deepCopyOf(ref)));
            }
          }
        }
      }
    } finally {
      reader.close();
    }
  }
}
