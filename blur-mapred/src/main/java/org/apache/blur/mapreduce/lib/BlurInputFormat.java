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
package org.apache.blur.mapreduce.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.blur.lucene.codec.Blur024Codec;
import org.apache.blur.manager.writer.SnapshotIndexDeletionPolicy;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

public class BlurInputFormat extends FileInputFormat<Text, TableBlurRecord> {
  private static final Log LOG = LogFactory.getLog(BlurInputFormat.class);

  private static final String BLUR_TABLE_PATH_MAPPING = "blur.table.path.mapping.";
  private static final String BLUR_INPUT_FORMAT_DISCOVERY_THREADS = "blur.input.format.discovery.threads";
  private static final String BLUR_TABLE_SNAPSHOT_MAPPING = "blur.table.snapshot.mapping.";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Path[] dirs = getInputPaths(context);
    Configuration configuration = context.getConfiguration();
    int threads = configuration.getInt(BLUR_INPUT_FORMAT_DISCOVERY_THREADS, 10);
    ExecutorService service = Executors.newFixedThreadPool(threads);
    try {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (Path dir : dirs) {
        Text table = BlurInputFormat.getTableFromPath(configuration, dir);
        String snapshot = getSnapshotForTable(configuration, table.toString());
        splits.addAll(getSegmentSplits(dir, service, configuration, table, new Text(snapshot)));
      }
      return splits;
    } finally {
      service.shutdownNow();
    }
  }

  public static void putPathToTable(Configuration configuration, String tableName, Path path) {
    configuration.set(BLUR_TABLE_PATH_MAPPING + tableName, path.toString());
  }

  public static Text getTableFromPath(Configuration configuration, Path path) throws IOException {
    for (Entry<String, String> e : configuration) {
      if (e.getKey().startsWith(BLUR_TABLE_PATH_MAPPING)) {
        String k = e.getKey();
        String table = k.substring(BLUR_TABLE_PATH_MAPPING.length());
        String pathStr = e.getValue();
        Path tablePath = new Path(pathStr);
        if (tablePath.equals(path)) {
          return new Text(table);
        }
      }
    }
    throw new IOException("Table name not found for path [" + path + "]");
  }

  public static void putSnapshotForTable(Configuration configuration, String tableName, String snapshot) {
    configuration.set(BLUR_TABLE_SNAPSHOT_MAPPING + tableName, snapshot);
  }

  public static String getSnapshotForTable(Configuration configuration, String tableName) throws IOException {
    for (Entry<String, String> e : configuration) {
      if (e.getKey().startsWith(BLUR_TABLE_SNAPSHOT_MAPPING)) {
        String k = e.getKey();
        String table = k.substring(BLUR_TABLE_SNAPSHOT_MAPPING.length());
        if (table.equals(tableName)) {
          return e.getValue();
        }
      }
    }
    throw new IOException("Snaphost not found for table [" + tableName + "]");
  }

  private List<InputSplit> getSegmentSplits(final Path dir, ExecutorService service, final Configuration configuration,
      final Text table, final Text snapshot) throws IOException {

    FileSystem fileSystem = dir.getFileSystem(configuration);
    FileStatus[] shardDirs = fileSystem.listStatus(dir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
      }
    });

    List<Future<List<InputSplit>>> futures = new ArrayList<Future<List<InputSplit>>>();
    for (final FileStatus shardFileStatus : shardDirs) {
      futures.add(service.submit(new Callable<List<InputSplit>>() {
        @Override
        public List<InputSplit> call() throws Exception {
          return getSegmentSplits(shardFileStatus.getPath(), configuration, table, snapshot);
        }
      }));
    }

    List<InputSplit> results = new ArrayList<InputSplit>();
    for (Future<List<InputSplit>> future : futures) {
      try {
        results.addAll(future.get());
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new IOException(cause);
        }
      }
    }
    return results;
  }

  private List<InputSplit> getSegmentSplits(Path shardDir, Configuration configuration, Text table, Text snapshot)
      throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    HdfsDirectory hdfsDirectory = new HdfsDirectory(configuration, shardDir);
    try {
      SnapshotIndexDeletionPolicy policy = new SnapshotIndexDeletionPolicy(configuration,
          SnapshotIndexDeletionPolicy.getGenerationsPath(shardDir));

      Long generation = policy.getGeneration(snapshot.toString());
      if (generation == null) {
        throw new IOException("Snapshot [" + snapshot + "] not found in shard [" + shardDir + "]");
      }

      List<IndexCommit> listCommits = DirectoryReader.listCommits(hdfsDirectory);
      IndexCommit indexCommit = findIndexCommit(listCommits, generation, shardDir);

      String segmentsFileName = indexCommit.getSegmentsFileName();
      SegmentInfos segmentInfos = new SegmentInfos();
      segmentInfos.read(hdfsDirectory, segmentsFileName);
      for (SegmentInfoPerCommit commit : segmentInfos) {
        SegmentInfo segmentInfo = commit.info;
        if (commit.getDelCount() == segmentInfo.getDocCount()) {
          LOG.info("Segment [" + segmentInfo.name + "] in dir [" + shardDir + "] has all records deleted.");
        } else {
          String name = segmentInfo.name;
          Set<String> files = segmentInfo.files();
          long fileLength = 0;
          for (String file : files) {
            fileLength += hdfsDirectory.fileLength(file);
          }
          splits.add(new BlurInputSplit(shardDir, segmentsFileName, name, fileLength, table));
        }
      }
      return splits;
    } finally {
      hdfsDirectory.close();
    }
  }

  private IndexCommit findIndexCommit(List<IndexCommit> listCommits, long generation, Path shardDir) throws IOException {
    for (IndexCommit commit : listCommits) {
      if (commit.getGeneration() == generation) {
        return commit;
      }
    }
    throw new IOException("Generation [" + generation + "] not found in shard [" + shardDir + "]");
  }

  @Override
  public RecordReader<Text, TableBlurRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    BlurRecordReader blurRecordReader = new BlurRecordReader();
    blurRecordReader.initialize(split, context);
    return blurRecordReader;
  }

  public static class BlurRecordReader extends RecordReader<Text, TableBlurRecord> {

    private boolean _setup;
    private Text _rowId;
    private TableBlurRecord _tableBlurRecord;
    private Bits _liveDocs;
    private StoredFieldsReader _fieldsReader;
    private HdfsDirectory _directory;

    private int _docId = -1;
    private int _maxDoc;
    private Text _table;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      if (_setup) {
        return;
      }
      _setup = true;

      Configuration configuration = context.getConfiguration();
      BlurInputSplit blurInputSplit = (BlurInputSplit) split;

      _table = blurInputSplit.getTable();

      _directory = new HdfsDirectory(configuration, blurInputSplit.getDir());
      SegmentInfos segmentInfos = new SegmentInfos();
      segmentInfos.read(_directory, blurInputSplit.getSegmentsName());
      SegmentInfoPerCommit commit = findSegmentInfoPerCommit(segmentInfos, blurInputSplit);

      Blur024Codec blur024Codec = new Blur024Codec();
      IOContext iocontext = IOContext.READ;
      SegmentInfo segmentInfo = commit.info;
      String segmentName = segmentInfo.name;
      FieldInfos fieldInfos = blur024Codec.fieldInfosFormat().getFieldInfosReader()
          .read(_directory, segmentName, iocontext);
      if (commit.getDelCount() > 0) {
        _liveDocs = blur024Codec.liveDocsFormat().readLiveDocs(_directory, commit, iocontext);
      }
      _fieldsReader = blur024Codec.storedFieldsFormat().fieldsReader(_directory, segmentInfo, fieldInfos, iocontext);

      _maxDoc = commit.info.getDocCount();
    }

    private SegmentInfoPerCommit findSegmentInfoPerCommit(SegmentInfos segmentInfos, BlurInputSplit blurInputSplit)
        throws IOException {
      String segmentInfoName = blurInputSplit.getSegmentInfoName();
      for (SegmentInfoPerCommit commit : segmentInfos) {
        if (commit.info.name.equals(segmentInfoName)) {
          return commit;
        }
      }
      throw new IOException("SegmentInfoPerCommit of [" + segmentInfoName + "] not found.");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (_docId >= _maxDoc) {
        return false;
      }
      while (true) {
        _docId++;
        if (_docId >= _maxDoc) {
          return false;
        }
        if (_liveDocs == null) {
          fetchBlurRecord();
          return true;
        } else if (_liveDocs.get(_docId)) {
          fetchBlurRecord();
          return true;
        }
      }
    }

    private void fetchBlurRecord() throws IOException {
      DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
      _fieldsReader.visitDocument(_docId, visitor);
      BlurRecord blurRecord = new BlurRecord();
      String rowId = RowDocumentUtil.readRecord(visitor.getDocument(), blurRecord);
      blurRecord.setRowId(rowId);
      _rowId = new Text(rowId);
      _tableBlurRecord = new TableBlurRecord(_table, blurRecord);
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return _rowId;
    }

    @Override
    public TableBlurRecord getCurrentValue() throws IOException, InterruptedException {
      return _tableBlurRecord;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return (float) _docId / (float) _maxDoc;
    }

    @Override
    public void close() throws IOException {
      _fieldsReader.close();
      _directory.close();
    }

  }

  public static class BlurInputSplit extends InputSplit implements Writable {

    private static final String UTF_8 = "UTF-8";
    private long _fileLength;
    private String _segmentsName;
    private Path _dir;
    private String _segmentInfoName;
    private Text _table = new Text();

    public BlurInputSplit() {

    }

    public BlurInputSplit(Path dir, String segmentsName, String segmentInfoName, long fileLength, Text table) {
      _fileLength = fileLength;
      _segmentsName = segmentsName;
      _segmentInfoName = segmentInfoName;
      _table = table;
      _dir = dir;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return _fileLength;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      // @TODO create locations for fdt file
      return new String[] {};
    }

    public String getSegmentInfoName() {
      return _segmentInfoName;
    }

    public String getSegmentsName() {
      return _segmentsName;
    }

    public Path getDir() {
      return _dir;
    }

    public Text getTable() {
      return _table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      writeString(out, _dir.toString());
      writeString(out, _segmentsName);
      writeString(out, _segmentInfoName);
      _table.write(out);
      out.writeLong(_fileLength);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _dir = new Path(readString(in));
      _segmentsName = readString(in);
      _segmentInfoName = readString(in);
      _table.readFields(in);
      _fileLength = in.readLong();
    }

    private void writeString(DataOutput out, String s) throws IOException {
      byte[] bs = s.getBytes(UTF_8);
      out.writeInt(bs.length);
      out.write(bs);
    }

    private String readString(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] buf = new byte[length];
      in.readFully(buf);
      return new String(buf, UTF_8);
    }

  }

  public static void addTable(Job job, TableDescriptor tableDescriptor, String snapshot)
      throws IllegalArgumentException, IOException {
    String tableName = tableDescriptor.getName();
    Path path = new Path(tableDescriptor.getTableUri());
    FileInputFormat.addInputPath(job, path);
    putPathToTable(job.getConfiguration(), tableName, path);
    putSnapshotForTable(job.getConfiguration(), tableName, snapshot);
  }

}
