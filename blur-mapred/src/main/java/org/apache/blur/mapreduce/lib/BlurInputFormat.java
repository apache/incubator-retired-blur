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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.blur.command.BlurArray;
import org.apache.blur.command.BlurObject;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.writer.SnapshotIndexDeletionPolicy;
import org.apache.blur.store.hdfs.DirectoryUtil;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

public class BlurInputFormat extends FileInputFormat<Text, TableBlurRecord> {
  private static final String BLUR_INPUT_FORMAT_MAX_MAPS = "blur.input.format.max.maps";

  private static final String BLUR_INPUTFORMAT_FILE_CACHE_PATH = "blur.inputformat.file.cache.path";

  private static final Log LOG = LogFactory.getLog(BlurInputFormat.class);

  private static final String BLUR_TABLE_PATH_MAPPING = "blur.table.path.mapping.";
  private static final String BLUR_INPUT_FORMAT_DISCOVERY_THREADS = "blur.input.format.discovery.threads";
  private static final String BLUR_TABLE_SNAPSHOT_MAPPING = "blur.table.snapshot.mapping.";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Path[] dirs = getInputPaths(context);
    List<BlurInputSplit> splits;
    Configuration configuration = context.getConfiguration();
    if (isSplitCommandSupported(configuration)) {
      splits = getSplitsFromCommand(configuration, dirs);
    } else {
      splits = getSplits(configuration, dirs);
    }
    return toList(getMaxNumberOfMaps(configuration), splits);
  }

  public static int getMaxNumberOfMaps(Configuration configuration) {
    return configuration.getInt(BLUR_INPUT_FORMAT_MAX_MAPS, Integer.MAX_VALUE);
  }

  public static void setMaxNumberOfMaps(Configuration configuration, int maxNumberOfMaps) {
    configuration.setInt(BLUR_INPUT_FORMAT_MAX_MAPS, maxNumberOfMaps);
  }

  public static void setMaxNumberOfMaps(Job job, int maxNumberOfMaps) {
    setMaxNumberOfMaps(job.getConfiguration(), maxNumberOfMaps);
  }

  private static List<BlurInputSplit> getSplitsFromCommand(Configuration configuration, Path[] dirs) throws IOException {
    String zkConnection = configuration.get(BlurConstants.BLUR_ZOOKEEPER_CONNECTION);
    Iface client = BlurClient.getClientFromZooKeeperConnectionStr(zkConnection);
    List<BlurInputSplit> splits = new ArrayList<BlurInputSplit>();
    for (Path dir : dirs) {
      Text table = BlurInputFormat.getTableFromPath(configuration, dir);
      String snapshot = getSnapshotForTable(configuration, table.toString());
      BlurInputFormatSplitCommand splitCommand = new BlurInputFormatSplitCommand();
      splitCommand.setSnapshot(snapshot);
      splitCommand.setTable(table.toString());
      List<BlurInputSplit> splitsList = toSplitList(splitCommand.run(client));
      splits.addAll(splitsList);
    }
    return splits;
  }

  private static List<BlurInputSplit> toSplitList(BlurObject bo) {
    Iterator<String> keys = bo.keys();
    List<BlurInputSplit> splits = new ArrayList<BlurInputSplit>();
    while (keys.hasNext()) {
      String shard = keys.next();
      BlurArray blurArray = bo.getBlurArray(shard);
      splits.addAll(toSplits(blurArray));
    }
    return splits;
  }

  public static List<BlurInputSplit> toSplits(BlurArray blurArray) {
    List<BlurInputSplit> splits = new ArrayList<BlurInputSplit>();
    for (int i = 0; i < blurArray.length(); i++) {
      BlurObject blurObject = blurArray.getBlurObject(i);
      splits.add(toSplit(blurObject));
    }
    return splits;
  }

  public static BlurArray toBlurArray(List<BlurInputSplit> splits) throws IOException {
    BlurArray blurArray = new BlurArray();
    for (BlurInputSplit inputSplit : splits) {
      blurArray.put(toBlurObject(inputSplit));
    }
    return blurArray;
  }

  private static BlurInputSplit toSplit(BlurObject blurObject) {
    Path dir = new Path(blurObject.getString("dir"));
    String segmentsName = blurObject.getString("segmentsName");
    String segmentInfoName = blurObject.getString("segmentInfoName");
    long fileLength = blurObject.getLong("fileLength");
    Text table = new Text(blurObject.getString("table"));
    List<String> directoryFiles = toStringListFromBlurArray(blurObject.getBlurArray("directoryFiles"));
    return new BlurInputSplit(dir, segmentsName, segmentInfoName, fileLength, table, directoryFiles);
  }

  private static List<String> toStringListFromBlurArray(BlurArray blurArray) {
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < blurArray.length(); i++) {
      list.add(blurArray.getString(i));
    }
    return list;
  }

  private static BlurObject toBlurObject(BlurInputSplit inputSplit) throws IOException {
    BlurObject blurObject = new BlurObject();
    blurObject.put("dir", inputSplit.getDir().toString());
    blurObject.put("segmentsName", inputSplit.getSegmentsName());
    blurObject.put("segmentInfoName", inputSplit.getSegmentInfoName());
    blurObject.put("fileLength", inputSplit.getLength());
    blurObject.put("table", inputSplit.getTable().toString());
    return blurObject;
  }

  private boolean isSplitCommandSupported(Configuration configuration) {
    return configuration.get(BlurConstants.BLUR_ZOOKEEPER_CONNECTION) != null;
  }

  private List<InputSplit> toList(int maxSplits, List<BlurInputSplit> splits) {
    // Reduce splits number requested
    List<BlurInputSplitColletion> collections = new ArrayList<BlurInputSplitColletion>();
    for (BlurInputSplit blurInputSplit : splits) {
      BlurInputSplitColletion blurInputSplitColletion;
      if (collections.size() < maxSplits) {
        blurInputSplitColletion = new BlurInputSplitColletion();
        collections.add(blurInputSplitColletion);
      } else {
        blurInputSplitColletion = findSmallest(collections);
      }
      blurInputSplitColletion.add(blurInputSplit);
    }

    List<InputSplit> inputSplits = new ArrayList<InputSplit>();
    for (BlurInputSplitColletion inputSplit : collections) {
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }

  private BlurInputSplitColletion findSmallest(List<BlurInputSplitColletion> collections) {
    Collections.sort(collections, new Comparator<BlurInputSplitColletion>() {
      @Override
      public int compare(BlurInputSplitColletion o1, BlurInputSplitColletion o2) {
        long l1 = o1.getLength();
        long l2 = o2.getLength();
        if (l1 == l2) {
          return 0;
        }
        // Smallest first
        return l1 < l2 ? -1 : 1;
      }
    });
    return collections.get(0);
  }

  public static class BlurInputSplitColletion extends InputSplit implements Writable {

    private List<BlurInputSplit> _splits = new ArrayList<BlurInputSplit>();
    private long _length;

    public BlurInputSplitColletion() {

    }

    public void add(BlurInputSplit blurInputSplit) {
      _splits.add(blurInputSplit);
      _length += blurInputSplit.getLength();
    }

    public BlurInputSplitColletion(List<BlurInputSplit> splits) {
      _splits = splits;
    }

    @Override
    public long getLength() {
      return _length;
    }

    @Override
    public String[] getLocations() {
      return new String[] {};
    }

    public List<BlurInputSplit> getSplits() {
      return _splits;
    }

    public void setSplits(List<BlurInputSplit> splits) {
      _splits = splits;
    }

    public void setLength(long length) {
      _length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(_length);
      out.writeInt(_splits.size());
      for (BlurInputSplit split : _splits) {
        split.write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _splits.clear();
      _length = in.readLong();
      int size = in.readInt();
      for (int i = 0; i < size; i++) {
        BlurInputSplit blurInputSplit = new BlurInputSplit();
        blurInputSplit.readFields(in);
        _splits.add(blurInputSplit);
      }
    }

  }

  public static List<BlurInputSplit> getSplits(Configuration configuration, Path[] dirs) throws IOException {
    int threads = configuration.getInt(BLUR_INPUT_FORMAT_DISCOVERY_THREADS, 10);
    ExecutorService service = Executors.newFixedThreadPool(threads);
    try {
      List<BlurInputSplit> splits = new ArrayList<BlurInputSplit>();
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

  private static List<BlurInputSplit> getSegmentSplits(final Path dir, ExecutorService service,
      final Configuration configuration, final Text table, final Text snapshot) throws IOException {

    FileSystem fileSystem = dir.getFileSystem(configuration);
    FileStatus[] shardDirs = fileSystem.listStatus(dir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
      }
    });

    List<Future<List<BlurInputSplit>>> futures = new ArrayList<Future<List<BlurInputSplit>>>();
    for (final FileStatus shardFileStatus : shardDirs) {
      futures.add(service.submit(new Callable<List<BlurInputSplit>>() {
        @Override
        public List<BlurInputSplit> call() throws Exception {
          return getSegmentSplits(shardFileStatus.getPath(), configuration, table, snapshot);
        }
      }));
    }

    List<BlurInputSplit> results = new ArrayList<BlurInputSplit>();
    for (Future<List<BlurInputSplit>> future : futures) {
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

  private static List<BlurInputSplit> getSegmentSplits(Path shardDir, Configuration configuration, Text table,
      Text snapshot) throws IOException {
    final long start = System.nanoTime();
    Directory directory = getDirectory(configuration, table.toString(), shardDir);
    try {
      return getSplitForDirectory(shardDir, configuration, table, snapshot, directory);
    } finally {
      directory.close();
      final long end = System.nanoTime();
      LOG.info("Found split in shard [{0}] in [{1} ms].", shardDir, (end - start) / 1000000000.0);
    }
  }

  public static List<BlurInputSplit> getSplitForDirectory(Path shardDir, Configuration configuration, String table,
      String snapshot, Directory directory) throws IOException {
    return getSplitForDirectory(shardDir, configuration, new Text(table), new Text(snapshot), directory);
  }

  public static List<BlurInputSplit> getSplitForDirectory(Path shardDir, Configuration configuration, Text table,
      Text snapshot, Directory directory) throws IOException {
    List<BlurInputSplit> splits = new ArrayList<BlurInputSplit>();
    SnapshotIndexDeletionPolicy policy = new SnapshotIndexDeletionPolicy(configuration,
        SnapshotIndexDeletionPolicy.getGenerationsPath(shardDir));

    Long generation = policy.getGeneration(snapshot.toString());
    if (generation == null) {
      throw new IOException("Snapshot [" + snapshot + "] not found in shard [" + shardDir + "]");
    }

    List<IndexCommit> listCommits = DirectoryReader.listCommits(directory);
    IndexCommit indexCommit = findIndexCommit(listCommits, generation, shardDir);

    String segmentsFileName = indexCommit.getSegmentsFileName();
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(directory, segmentsFileName);
    for (SegmentInfoPerCommit commit : segmentInfos) {
      SegmentInfo segmentInfo = commit.info;
      if (commit.getDelCount() == segmentInfo.getDocCount()) {
        LOG.info("Segment [{0}] in dir [{1}] has all records deleted.", segmentInfo.name, shardDir);
      } else {
        String name = segmentInfo.name;
        Collection<String> files = commit.files();
        long fileLength = 0;
        for (String file : files) {
          fileLength += directory.fileLength(file);
        }
        List<String> dirFiles = new ArrayList<String>(files);
        dirFiles.add(segmentsFileName);
        splits.add(new BlurInputSplit(shardDir, segmentsFileName, name, fileLength, table, dirFiles));
      }
    }
    return splits;
  }

  private static IndexCommit findIndexCommit(List<IndexCommit> listCommits, long generation, Path shardDir)
      throws IOException {
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
    final GenericRecordReaderCollection genericRecordReader = new GenericRecordReaderCollection();
    genericRecordReader.initialize((BlurInputSplitColletion) split, context.getConfiguration());
    return new RecordReader<Text, TableBlurRecord>() {

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        genericRecordReader.initialize((BlurInputSplitColletion) split, context.getConfiguration());
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return genericRecordReader.nextKeyValue();
      }

      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        return genericRecordReader.getCurrentKey();
      }

      @Override
      public TableBlurRecord getCurrentValue() throws IOException, InterruptedException {
        return genericRecordReader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return genericRecordReader.getProgress();
      }

      @Override
      public void close() throws IOException {
        genericRecordReader.close();
      }

    };
  }

  public static class BlurInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit, Writable {

    private static final String UTF_8 = "UTF-8";
    private long _fileLength;
    private String _segmentsName;
    private Path _dir;
    private String _segmentInfoName;
    private Text _table = new Text();
    private List<String> _directoryFiles;

    public BlurInputSplit() {

    }

    public BlurInputSplit(Path dir, String segmentsName, String segmentInfoName, long fileLength, Text table,
        List<String> directoryFiles) {
      _fileLength = fileLength;
      _segmentsName = segmentsName;
      _segmentInfoName = segmentInfoName;
      _table = table;
      _dir = dir;
    }

    @Override
    public long getLength() {
      return _fileLength;
    }

    @Override
    public String[] getLocations() {
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
      int size = _directoryFiles.size();
      out.writeInt(size);
      for (String s : _directoryFiles) {
        writeString(out, s);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _dir = new Path(readString(in));
      _segmentsName = readString(in);
      _segmentInfoName = readString(in);
      _table.readFields(in);
      _fileLength = in.readLong();
      int size = in.readInt();
      _directoryFiles = new ArrayList<String>();
      for (int i = 0; i < size; i++) {
        _directoryFiles.add(readString(in));
      }
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

  public static void setLocalCachePath(Job job, Path fileCachePath) {
    setLocalCachePath(job.getConfiguration(), fileCachePath);
  }

  public static void setLocalCachePath(Configuration configuration, Path fileCachePath) {
    configuration.set(BLUR_INPUTFORMAT_FILE_CACHE_PATH, fileCachePath.toString());
  }

  public static Path getLocalCachePath(Configuration configuration) {
    String p = configuration.get(BLUR_INPUTFORMAT_FILE_CACHE_PATH);
    if (p == null) {
      return null;
    }
    return new Path(p);
  }

  public static void addTable(Job job, TableDescriptor tableDescriptor, String snapshot)
      throws IllegalArgumentException, IOException {
    String tableName = tableDescriptor.getName();
    Path path = new Path(tableDescriptor.getTableUri());
    FileInputFormat.addInputPath(job, path);
    putPathToTable(job.getConfiguration(), tableName, path);
    putSnapshotForTable(job.getConfiguration(), tableName, snapshot);
  }

  public static Directory getDirectory(Configuration configuration, String table, Path shardDir) throws IOException {
    Path fastPath = DirectoryUtil.getFastDirectoryPath(shardDir);
    FileSystem fileSystem = shardDir.getFileSystem(configuration);
    boolean disableFast = !fileSystem.exists(fastPath);
    HdfsDirectory directory = new HdfsDirectory(configuration, shardDir, null);
    return DirectoryUtil.getDirectory(configuration, directory, disableFast, null, table, shardDir.getName(), true);
  }

  public static void setZooKeeperConnectionStr(Configuration configuration, String zk) {
    configuration.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, zk);
  }

  public static void setZooKeeperConnectionStr(Job job, String zk) {
    setZooKeeperConnectionStr(job.getConfiguration(), zk);
  }
}
