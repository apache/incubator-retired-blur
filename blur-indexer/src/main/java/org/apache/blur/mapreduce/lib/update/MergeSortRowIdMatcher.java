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
package org.apache.blur.mapreduce.lib.update;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.index.AtomicReaderUtil;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class MergeSortRowIdMatcher {

  private static final String DEL = ".del";
  private static final Log LOG = LogFactory.getLog(MergeSortRowIdMatcher.class);
  private static final Progressable NO_OP = new Progressable() {
    @Override
    public void progress() {

    }
  };
  private static final long _10_SECONDS = TimeUnit.SECONDS.toNanos(10);

  public interface Action {
    void found(Text rowId) throws IOException;
  }

  private final MyReader[] _readers;
  private final Configuration _configuration;
  private final Path _cachePath;
  private final IndexCommit _indexCommit;
  private final Directory _directory;
  private final Progressable _progressable;

  private DirectoryReader _reader;

  public MergeSortRowIdMatcher(Directory directory, long generation, Configuration configuration, Path cachePath)
      throws IOException {
    this(directory, generation, configuration, cachePath, null);
  }

  public MergeSortRowIdMatcher(Directory directory, long generation, Configuration configuration, Path cachePath,
      Progressable progressable) throws IOException {
    List<IndexCommit> listCommits = DirectoryReader.listCommits(directory);
    _indexCommit = findIndexCommit(listCommits, generation);
    _configuration = configuration;
    _cachePath = cachePath;
    _directory = directory;
    _progressable = progressable == null ? NO_OP : progressable;
    _readers = openReaders();
  }

  public void lookup(Text rowId, Action action) throws IOException {
    if (lookup(rowId)) {
      action.found(rowId);
    }
  }

  private boolean lookup(Text rowId) throws IOException {
    advanceReadersIfNeeded(rowId);
    sortReaders();
    return checkReaders(rowId);
  }

  private boolean checkReaders(Text rowId) {
    for (MyReader reader : _readers) {
      int compareTo = reader.getCurrentRowId().compareTo(rowId);
      if (compareTo == 0) {
        return true;
      } else if (compareTo > 0) {
        return false;
      }
    }
    return false;
  }

  private void advanceReadersIfNeeded(Text rowId) throws IOException {
    _progressable.progress();
    for (MyReader reader : _readers) {
      if (rowId.compareTo(reader.getCurrentRowId()) > 0) {
        advanceReader(reader, rowId);
      }
    }
  }

  private void advanceReader(MyReader reader, Text rowId) throws IOException {
    while (reader.next()) {
      if (rowId.compareTo(reader.getCurrentRowId()) <= 0) {
        return;
      }
    }
  }

  private static final Comparator<MyReader> COMP = new Comparator<MyReader>() {
    @Override
    public int compare(MyReader o1, MyReader o2) {
      return o1.getCurrentRowId().compareTo(o2.getCurrentRowId());
    }
  };

  private void sortReaders() {
    Arrays.sort(_readers, COMP);
  }

  private MyReader[] openReaders() throws IOException {
    Collection<SegmentKey> segmentKeys = getSegmentKeys();
    MyReader[] readers = new MyReader[segmentKeys.size()];
    int i = 0;
    for (SegmentKey segmentKey : segmentKeys) {
      readers[i++] = openReader(segmentKey);
    }
    return readers;
  }

  private MyReader openReader(SegmentKey segmentKey) throws IOException {
    Path file = getCacheFilePath(segmentKey);
    FileSystem fileSystem = _cachePath.getFileSystem(_configuration);
    if (!fileSystem.exists(file)) {
      createCacheFile(file, segmentKey);
    }
    Reader reader = new SequenceFile.Reader(_configuration, SequenceFile.Reader.file(file));
    return new MyReader(reader);
  }

  private void createCacheFile(Path file, SegmentKey segmentKey) throws IOException {
    LOG.info("Building cache for segment [{0}] to [{1}]", segmentKey, file);
    Path tmpPath = getTmpWriterPath(file.getParent());
    try (Writer writer = createWriter(_configuration, tmpPath)) {
      DirectoryReader reader = getReader();
      for (AtomicReaderContext context : reader.leaves()) {
        SegmentReader segmentReader = AtomicReaderUtil.getSegmentReader(context.reader());
        if (segmentReader.getSegmentName().equals(segmentKey.getSegmentName())) {
          writeRowIds(writer, segmentReader);
          break;
        }
      }
    }
    commitWriter(_configuration, file, tmpPath);
  }

  public static void commitWriter(Configuration configuration, Path file, Path tmpPath) throws IOException {
    FileSystem fileSystem = tmpPath.getFileSystem(configuration);
    LOG.info("Commit tmp [{0}] to file [{1}]", tmpPath, file);
    if (!fileSystem.rename(tmpPath, file)) {
      LOG.warn("Could not commit tmp file [{0}] to file [{1}]", tmpPath, file);
    }
  }

  public static Path getTmpWriterPath(Path dir) {
    return new Path(dir, UUID.randomUUID().toString() + ".tmp");
  }

  public static Writer createWriter(Configuration configuration, Path tmpPath) throws IOException {
    return SequenceFile.createWriter(configuration, SequenceFile.Writer.file(tmpPath),
        SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(NullWritable.class),
        SequenceFile.Writer.compression(CompressionType.BLOCK, getCodec(configuration)));
  }

  private static CompressionCodec getCodec(Configuration configuration) {
    if (ZlibFactory.isNativeZlibLoaded(configuration)) {
      return new GzipCodec();
    }
    return new DeflateCodec();
  }

  private void writeRowIds(Writer writer, SegmentReader segmentReader) throws IOException {
    Terms terms = segmentReader.terms(BlurConstants.ROW_ID);
    if (terms == null) {
      return;
    }
    TermsEnum termsEnum = terms.iterator(null);
    BytesRef rowId;
    long s = System.nanoTime();
    while ((rowId = termsEnum.next()) != null) {
      long n = System.nanoTime();
      if (n + _10_SECONDS > s) {
        _progressable.progress();
        s = System.nanoTime();
      }
      writer.append(new Text(rowId.utf8ToString()), NullWritable.get());
    }
  }

  private IndexCommit findIndexCommit(List<IndexCommit> listCommits, long generation) throws IOException {
    for (IndexCommit commit : listCommits) {
      if (commit.getGeneration() == generation) {
        return commit;
      }
    }
    throw new IOException("Generation [" + generation + "] not found.");
  }

  static class SegmentKey {

    final String _segmentName;
    final String _id;

    SegmentKey(String segmentName, String id) throws IOException {
      _segmentName = segmentName;
      _id = id;
    }

    String getSegmentName() {
      return _segmentName;
    }

    @Override
    public String toString() {
      return _id;
    }
  }

  private DirectoryReader getReader() throws IOException {
    if (_reader == null) {
      _reader = DirectoryReader.open(_indexCommit);
    }
    return _reader;
  }

  private Collection<SegmentKey> getSegmentKeys() throws IOException {
    List<SegmentKey> keys = new ArrayList<SegmentKey>();
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(_directory, _indexCommit.getSegmentsFileName());
    for (SegmentInfoPerCommit segmentInfoPerCommit : segmentInfos) {
      String name = segmentInfoPerCommit.info.name;
      String id = getId(segmentInfoPerCommit.info);
      keys.add(new SegmentKey(name, id));
    }
    return keys;
  }

  private String getId(SegmentInfo si) throws IOException {
    HdfsDirectory dir = getHdfsDirectory(si.dir);
    Set<String> files = new TreeSet<String>(si.files());
    return getId(_configuration, dir, files);
  }

  private static String getId(Configuration configuration, HdfsDirectory dir, Set<String> files) throws IOException {
    long ts = 0;
    String file = null;
    for (String f : files) {
      if (f.endsWith(DEL)) {
        continue;
      }
      long fileModified = dir.getFileModified(f);
      if (fileModified > ts) {
        ts = fileModified;
        file = f;
      }
    }

    Path path = dir.getPath();
    FileSystem fileSystem = path.getFileSystem(configuration);
    Path realFile = new Path(path, file);
    if (!fileSystem.exists(realFile)) {
      realFile = dir.getRealFilePathFromSymlink(file);
      if (!fileSystem.exists(realFile)) {
        throw new IOException("Lucene file [" + file + "] for dir [" + path + "] can not be found.");
      }
    }
    return getFirstBlockId(fileSystem, realFile);
  }

  public static String getIdForSingleSegmentIndex(Configuration configuration, Path indexPath) throws IOException {
    HdfsDirectory dir = new HdfsDirectory(configuration, indexPath);
    Set<String> files = new TreeSet<String>(Arrays.asList(dir.listAll()));
    return getId(configuration, dir, files);
  }

  private static String getFirstBlockId(FileSystem fileSystem, Path realFile) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(realFile);
    BlockLocation[] locations = fileSystem.getFileBlockLocations(fileStatus, 0, 1);
    HdfsBlockLocation location = (HdfsBlockLocation) locations[0];
    LocatedBlock locatedBlock = location.getLocatedBlock();
    ExtendedBlock block = locatedBlock.getBlock();
    return toNiceString(block.getBlockId());
  }

  private static String toNiceString(long blockId) {
    return "b" + blockId;
  }

  private static HdfsDirectory getHdfsDirectory(Directory dir) {
    if (dir instanceof HdfsDirectory) {
      return (HdfsDirectory) dir;
    } else if (dir instanceof DirectoryDecorator) {
      DirectoryDecorator dd = (DirectoryDecorator) dir;
      return getHdfsDirectory(dd.getOriginalDirectory());
    } else {
      throw new RuntimeException("Unknown directory type.");
    }
  }

  private Path getCacheFilePath(SegmentKey segmentKey) {
    return new Path(_cachePath, segmentKey + ".seq");
  }

  static class MyReader {

    final Reader _reader;
    final Text _rowId = new Text();
    boolean _finished = false;

    public MyReader(Reader reader) {
      _reader = reader;
    }

    public Text getCurrentRowId() {
      return _rowId;
    }

    public boolean next() throws IOException {
      if (_finished) {
        return false;
      }
      if (_reader.next(_rowId)) {
        return true;
      }
      _finished = true;
      return false;
    }

    public boolean isFinished() {
      return _finished;
    }
  }

  public static Path getCachePath(Path cachePath, String table, String shardName) {
    return new Path(new Path(cachePath, table), shardName);
  }
}
