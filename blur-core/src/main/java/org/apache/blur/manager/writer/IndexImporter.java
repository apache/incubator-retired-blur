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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.ShardUtil;
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

  private static final String INPROGRESS = ".inprogress";
  private static final String BADROWIDS = ".badrowids";
  private static final String COMMIT = ".commit";
  private static final String INUSE = ".inuse";
  private static final String BADINDEX = ".badindex";
  private static final Lock _globalLock = new ReentrantReadWriteLock().writeLock();

  private final static Log LOG = LogFactory.getLog(IndexImporter.class);

  private final BlurIndex _blurIndex;
  private final ShardContext _shardContext;
  private final String _table;
  private final String _shard;
  private final long _cleanupDelay;
  private final Timer _inindexImporterTimer;
  private final ThriftCache _thriftCache;
  private final HdfsDirectory _directory;

  private long _lastCleanup;
  private Runnable _testError;

  public IndexImporter(Timer indexImporterTimer, BlurIndex blurIndex, ShardContext shardContext, TimeUnit refreshUnit,
      long refreshAmount, ThriftCache thriftCache, Directory dir) throws IOException {
    _thriftCache = thriftCache;
    _blurIndex = blurIndex;
    _shardContext = shardContext;
    _directory = getHdfsDirectory(dir);

    long period = refreshUnit.toMillis(refreshAmount);
    indexImporterTimer.schedule(this, period, period);
    _inindexImporterTimer = indexImporterTimer;
    _table = _shardContext.getTableContext().getTable();
    _shard = _shardContext.getShard();
    _cleanupDelay = TimeUnit.MINUTES.toMillis(10);
  }

  private HdfsDirectory getHdfsDirectory(Directory dir) throws IOException {
    if (dir instanceof HdfsDirectory) {
      return (HdfsDirectory) dir;
    } else if (dir instanceof DirectoryDecorator) {
      DirectoryDecorator decorator = (DirectoryDecorator) dir;
      return getHdfsDirectory(decorator.getOriginalDirectory());
    } else {
      throw new IOException("Directory [" + dir + "] is not HdfsDirectory or DirectoryDecorator");
    }
  }

  @Override
  public void close() throws IOException {
    cancel();
    _inindexImporterTimer.purge();
  }

  public long getSegmentImportPendingCount() throws IOException {
    Path path = _shardContext.getHdfsDirPath();
    Configuration configuration = _shardContext.getTableContext().getConfiguration();
    FileSystem fileSystem = path.getFileSystem(configuration);
    for (int i = 0; i < 10; i++) {
      try {
        FileStatus[] listStatus = fileSystem.listStatus(path, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (path != null && path.getName().endsWith(COMMIT)) {
              return true;
            }
            return false;
          }
        });
        return listStatus.length;
      } catch (FileNotFoundException e) {
        LOG.warn("File not found error, retrying.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return 0L;
      }
    }
    throw new IOException("Received too many errors. Give up.");
  }

  public long getSegmentImportInProgressCount() throws IOException {
    Path path = _shardContext.getHdfsDirPath();
    Configuration configuration = _shardContext.getTableContext().getConfiguration();
    FileSystem fileSystem = path.getFileSystem(configuration);
    for (int i = 0; i < 10; i++) {
      try {
        FileStatus[] listStatus = fileSystem.listStatus(path, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (path != null && path.getName().endsWith(INUSE)) {
              return true;
            }
            return false;
          }
        });
        long count = 0;
        for (FileStatus fileStatus : listStatus) {
          Path p = fileStatus.getPath();
          if (fileSystem.exists(new Path(p, INPROGRESS))) {
            count++;
          }
        }
        return count;
      } catch (FileNotFoundException e) {
        LOG.warn("File not found error, retrying.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        return 0L;
      }
    }
    throw new IOException("Received too many errors. Give up.");
  }

  @Override
  public void run() {
    // Only allow one import to occur in the process at a time.
    _globalLock.lock();
    try {
      if (_lastCleanup + _cleanupDelay < System.currentTimeMillis()) {
        try {
          cleanupOldDirs();
        } catch (IOException e) {
          LOG.error("Unknown error while trying to clean old directories on [{1}/{2}].", e, _shard, _table);
        }
        _lastCleanup = System.currentTimeMillis();
      }
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
                if (path != null && path.getName().endsWith(COMMIT)) {
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
        for (FileStatus fileStatus : listStatus) {
          Path file = fileStatus.getPath();
          if (fileStatus.isDir() && file.getName().endsWith(COMMIT)) {
            // rename to inuse, if good continue else rename to badindex
            Path inuse = new Path(file.getParent(), rename(file.getName(), INUSE));
            touch(fileSystem, new Path(file, INPROGRESS));
            if (fileSystem.rename(file, inuse)) {
              if (_testError != null) {
                _testError.run();
              }
              HdfsDirectory hdfsDirectory = new HdfsDirectory(configuration, inuse);
              try {
                if (DirectoryReader.indexExists(hdfsDirectory)) {
                  IndexAction indexAction = getIndexAction(hdfsDirectory, fileSystem);
                  _blurIndex.process(indexAction);
                  return;
                } else {
                  Path badindex = new Path(file.getParent(), rename(file.getName(), BADINDEX));
                  if (fileSystem.rename(inuse, badindex)) {
                    LOG.error("Directory found at [{0}] is not a vaild index, renaming to [{1}].", inuse, badindex);
                  } else {
                    LOG.fatal("Directory found at [{0}] is not a vaild index, could not rename to [{1}].", inuse,
                        badindex);
                  }
                }
              } finally {
                hdfsDirectory.close();
              }
            } else {
              LOG.fatal("Could not rename [{0}] to inuse dir.", file);
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Unknown error while trying to refresh imports on [{1}/{2}].", e, _shard, _table);
      }
    } finally {
      _globalLock.unlock();
    }
  }

  private void touch(FileSystem fileSystem, Path path) throws IOException {
    fileSystem.create(path, true).close();
  }

  private String rename(String name, String newSuffix) {
    int lastIndexOf = name.lastIndexOf('.');
    return name.substring(0, lastIndexOf) + newSuffix;
  }

  private IndexAction getIndexAction(final HdfsDirectory directory, final FileSystem fileSystem) {
    return new IndexAction() {

      @Override
      public void performMutate(IndexSearcherCloseable searcher, IndexWriter writer) throws IOException {
        LOG.info("About to import [{0}] into [{1}/{2}]", directory, _shard, _table);
        boolean emitDeletes = searcher.getIndexReader().numDocs() != 0;
        applyDeletes(directory, writer, _shard, emitDeletes);
        LOG.info("Add index [{0}] [{1}/{2}]", directory, _shard, _table);
        writer.addIndexes(directory);
        LOG.info("Removing delete markers [{0}] on [{1}/{2}]", directory, _shard, _table);
        writer.deleteDocuments(new Term(BlurConstants.DELETE_MARKER, BlurConstants.DELETE_MARKER_VALUE));
        LOG.info("Finishing import [{0}], commiting on [{1}/{2}]", directory, _shard, _table);
      }

      @Override
      public void doPreCommit(IndexSearcherCloseable indexSearcher, IndexWriter writer) throws IOException {

      }

      @Override
      public void doPostCommit(IndexWriter writer) throws IOException {
        Path path = directory.getPath();
        fileSystem.delete(new Path(path, INPROGRESS), false);
        LOG.info("Import complete on [{0}/{1}]", _shard, _table);
        writer.maybeMerge();
      }

      @Override
      public void doPreRollback(IndexWriter writer) throws IOException {
        LOG.info("Starting rollback on [{0}/{1}]", _shard, _table);
      }

      @Override
      public void doPostRollback(IndexWriter writer) throws IOException {
        LOG.info("Finished rollback on [{0}/{1}]", _shard, _table);
        Path path = directory.getPath();
        String name = path.getName();
        fileSystem.rename(path, new Path(path.getParent(), rename(name, BADROWIDS)));
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
      int shardId = ShardUtil.getShardIndex(shard);
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

  public void cleanupOldDirs() throws IOException {
    Path hdfsDirPath = _shardContext.getHdfsDirPath();
    TableContext tableContext = _shardContext.getTableContext();
    Configuration configuration = tableContext.getConfiguration();
    FileSystem fileSystem = hdfsDirPath.getFileSystem(configuration);
    FileStatus[] inuseSubDirs = fileSystem.listStatus(hdfsDirPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(INUSE);
      }
    });
    Set<Path> inuseDirs = toSet(inuseSubDirs);
    Map<Path, Path> inuseFileToDir = toMap(fileSystem, inuseDirs);
    FileStatus[] listStatus = fileSystem.listStatus(hdfsDirPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(HdfsDirectory.LNK);
      }
    });

    for (FileStatus status : listStatus) {
      String realFileName = HdfsDirectory.getRealFileName(status.getPath().getName());
      Path realPath = _directory.getRealFilePathFromSymlink(realFileName);
      Path inuseDir = inuseFileToDir.get(realPath);
      inuseDirs.remove(inuseDir);
      // if the inuse dir has an inprogress file then remove it because there
      // are files that reference this dir so it had to be committed.
      Path path = new Path(inuseDir, INPROGRESS);
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, false);
        if (_thriftCache != null) {
          _thriftCache.clearTable(_table);
        }
      }
    }

    // Check if any inuse dirs have inprogress files.
    // If they do, rename inuse to commit to retry import.
    for (Path inuse : new HashSet<Path>(inuseDirs)) {
      Path path = new Path(inuse, INPROGRESS);
      if (fileSystem.exists(path)) {
        LOG.info("Path [{0}] is not imported but has inprogress file, retrying import.", path);
        inuseDirs.remove(inuse);
        Path commit = new Path(inuse.getParent(), rename(inuse.getName(), COMMIT));
        fileSystem.rename(inuse, commit);
      }
    }

    for (Path p : inuseDirs) {
      LOG.info("Deleting path [{0}] no longer in use.", p);
      fileSystem.delete(p, true);
    }
  }

  private Map<Path, Path> toMap(FileSystem fileSystem, Set<Path> inuseDirs) throws IOException {
    Map<Path, Path> result = new TreeMap<Path, Path>();
    for (Path p : inuseDirs) {
      if (!fileSystem.isFile(p)) {
        FileStatus[] listStatus = fileSystem.listStatus(p);
        for (FileStatus status : listStatus) {
          result.put(status.getPath(), p);
        }
      }
    }
    return result;
  }

  private Set<Path> toSet(FileStatus[] dirs) {
    Set<Path> result = new TreeSet<Path>();
    for (FileStatus status : dirs) {
      result.add(status.getPath());
    }
    return result;
  }

  public Runnable getTestError() {
    return _testError;
  }

  public void setTestError(Runnable testError) {
    _testError = testError;
  }

}
