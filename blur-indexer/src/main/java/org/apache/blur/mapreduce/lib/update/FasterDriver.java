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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurInputFormat;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FasterDriver extends Configured implements Tool {

  public static final String BLUR_UPDATE_ID = "blur.update.id";
  private static final String BLUR_EXEC_TYPE = "blur.exec.type";
  public static final String TMP = "tmp";

  public enum EXEC {
    MR_ONLY, MR_WITH_LOOKUP, AUTOMATIC
  }

  public static final String MRUPDATE_SNAPSHOT = "mrupdate-snapshot";
  public static final String CACHE = "cache";
  public static final String COMPLETE = "complete";
  public static final String INPROGRESS = "inprogress";
  public static final String NEW = "new";
  private static final Log LOG = LogFactory.getLog(FasterDriver.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FasterDriver(), args);
    System.exit(res);
  }

  static class PartitionedInputResult {
    final Path _partitionedInputData;
    final Counters _counters;
    final long[] _rowIdsFromNewData;
    final long[] _rowIdsToUpdateFromNewData;
    final long[] _rowIdsFromIndex;

    PartitionedInputResult(Path partitionedInputData, Counters counters, int shards, TaskReport[] taskReports) {
      _partitionedInputData = partitionedInputData;
      _counters = counters;
      _rowIdsFromNewData = new long[shards];
      _rowIdsToUpdateFromNewData = new long[shards];
      _rowIdsFromIndex = new long[shards];
      for (TaskReport tr : taskReports) {
        int id = tr.getTaskID().getId();
        Counters taskCounters = tr.getTaskCounters();
        Counter total = taskCounters.findCounter(BlurIndexCounter.ROW_IDS_FROM_NEW_DATA);
        _rowIdsFromNewData[id] = total.getValue();
        Counter update = taskCounters.findCounter(BlurIndexCounter.ROW_IDS_TO_UPDATE_FROM_NEW_DATA);
        _rowIdsToUpdateFromNewData[id] = update.getValue();
        Counter index = taskCounters.findCounter(BlurIndexCounter.ROW_IDS_FROM_INDEX);
        _rowIdsFromIndex[id] = index.getValue();
      }
    }

  }

  @Override
  public int run(String[] args) throws Exception {
    int c = 0;
    if (args.length < 5) {
      System.err
          .println("Usage Driver <table> <mr inc working path> <output path> <zk connection> <reducer multipler> <extra config files...>");
      return 1;
    }
    String table = args[c++];
    String mrIncWorkingPathStr = args[c++];
    String outputPathStr = args[c++];
    String blurZkConnection = args[c++];
    int reducerMultipler = Integer.parseInt(args[c++]);
    for (; c < args.length; c++) {
      String externalConfigFileToAdd = args[c];
      getConf().addResource(new Path(externalConfigFileToAdd));
    }

    Path outputPath = new Path(outputPathStr);
    Path mrIncWorkingPath = new Path(mrIncWorkingPathStr);
    FileSystem fileSystem = mrIncWorkingPath.getFileSystem(getConf());

    Path newData = new Path(mrIncWorkingPath, NEW);
    Path inprogressData = new Path(mrIncWorkingPath, INPROGRESS);
    Path completeData = new Path(mrIncWorkingPath, COMPLETE);
    Path fileCache = new Path(mrIncWorkingPath, CACHE);
    Path tmpPathDontDelete = new Path(mrIncWorkingPath, TMP);

    Path tmpPath = new Path(tmpPathDontDelete, UUID.randomUUID().toString());

    fileSystem.mkdirs(newData);
    fileSystem.mkdirs(inprogressData);
    fileSystem.mkdirs(completeData);
    fileSystem.mkdirs(fileCache);

    List<Path> srcPathList = new ArrayList<Path>();
    for (FileStatus fileStatus : fileSystem.listStatus(newData)) {
      srcPathList.add(fileStatus.getPath());
    }
    if (srcPathList.isEmpty()) {
      return 0;
    }

    List<Path> inprogressPathList = new ArrayList<Path>();
    boolean success = false;
    Iface client = null;

    EXEC exec = EXEC.valueOf(getConf().get(BLUR_EXEC_TYPE, EXEC.AUTOMATIC.name()).toUpperCase());

    String uuid = UUID.randomUUID().toString();

    try {
      client = BlurClient.getClientFromZooKeeperConnectionStr(blurZkConnection);
      TableDescriptor descriptor = client.describe(table);
      Map<String, String> tableProperties = descriptor.getTableProperties();
      String fastDir = tableProperties.get("blur.table.disable.fast.dir");
      if (fastDir == null || !fastDir.equals("true")) {
        LOG.error("Table [{0}] has blur.table.disable.fast.dir enabled, not supported in fast MR update.", table);
        return 1;
      }

      waitForOtherSnapshotsToBeRemoved(client, table, MRUPDATE_SNAPSHOT);
      client.createSnapshot(table, MRUPDATE_SNAPSHOT);
      TableStats tableStats = client.tableStats(table);

      inprogressPathList = movePathList(fileSystem, inprogressData, srcPathList);

      switch (exec) {
      case MR_ONLY:
        success = runMrOnly(descriptor, inprogressPathList, table, fileCache, outputPath, reducerMultipler);
        break;
      case MR_WITH_LOOKUP:
        success = runMrWithLookup(uuid, descriptor, inprogressPathList, table, fileCache, outputPath, reducerMultipler,
            tmpPath, tableStats, MRUPDATE_SNAPSHOT);
        break;
      case AUTOMATIC:
        success = runAutomatic(uuid, descriptor, inprogressPathList, table, fileCache, outputPath, reducerMultipler,
            tmpPath, tableStats, MRUPDATE_SNAPSHOT);
        break;
      default:
        throw new RuntimeException("Exec type [" + exec + "] not supported.");
      }
    } finally {
      if (success) {
        LOG.info("Associate lookup cache with new data!");
        associateLookupCache(uuid, fileCache, outputPath);
        LOG.info("Indexing job succeeded!");
        client.loadData(table, outputPathStr);
        LOG.info("Load data called");
        movePathList(fileSystem, completeData, inprogressPathList);
        LOG.info("Input data moved to complete");
        ClusterDriver.waitForDataToLoad(client, table);
        LOG.info("Data loaded");
      } else {
        LOG.error("Indexing job failed!");
        movePathList(fileSystem, newData, inprogressPathList);
      }
      fileSystem.delete(tmpPath, true);
      if (client != null) {
        client.removeSnapshot(table, MRUPDATE_SNAPSHOT);
      }
    }

    if (success) {
      return 0;
    } else {
      return 1;
    }
  }

  private void associateLookupCache(String uuid, Path fileCache, Path outputPath) throws IOException {
    FileSystem fileSystem = fileCache.getFileSystem(getConf());
    cleanupExtraFileFromSpecX(fileSystem, uuid, fileCache);
    associateLookupCache(fileSystem, uuid, fileSystem.getFileStatus(fileCache), outputPath);
  }

  private void cleanupExtraFileFromSpecX(FileSystem fileSystem, String uuid, Path fileCache) throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(fileCache);
    List<FileStatus> uuidPaths = new ArrayList<FileStatus>();
    for (FileStatus fs : listStatus) {
      Path path = fs.getPath();
      if (fs.isDirectory()) {
        cleanupExtraFileFromSpecX(fileSystem, uuid, path);
      } else if (path.getName().startsWith(uuid)) {
        uuidPaths.add(fs);
      }
    }
    if (uuidPaths.size() > 1) {
      deleteIncomplete(fileSystem, uuidPaths);
    }
  }

  private void deleteIncomplete(FileSystem fileSystem, List<FileStatus> uuidPaths) throws IOException {
    long max = 0;
    FileStatus keeper = null;
    for (FileStatus fs : uuidPaths) {
      long len = fs.getLen();
      if (len > max) {
        keeper = fs;
        max = len;
      }
    }
    for (FileStatus fs : uuidPaths) {
      if (fs != keeper) {
        LOG.info("Deleteing incomplete cache file [{0}]", fs.getPath());
        fileSystem.delete(fs.getPath(), false);
      }
    }
  }

  private void associateLookupCache(FileSystem fileSystem, String uuid, FileStatus fileCache, Path outputPath)
      throws IOException {
    Path path = fileCache.getPath();
    if (fileCache.isDirectory()) {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fs : listStatus) {
        associateLookupCache(fileSystem, uuid, fs, outputPath);
      }
    } else if (path.getName().startsWith(uuid)) {
      Path parent = path.getParent();
      String shardName = parent.getName();
      Path indexPath = findOutputDirPath(outputPath, shardName);
      LOG.info("Path found for shard [{0}] outputPath [{1}]", shardName, outputPath);
      String id = MergeSortRowIdMatcher.getIdForSingleSegmentIndex(getConf(), indexPath);
      Path file = new Path(path.getParent(), id + ".seq");
      MergeSortRowIdMatcher.commitWriter(getConf(), file, path);
    }
  }

  private Path findOutputDirPath(Path outputPath, String shardName) throws IOException {
    FileSystem fileSystem = outputPath.getFileSystem(getConf());
    Path shardPath = new Path(outputPath, shardName);
    if (!fileSystem.exists(shardPath)) {
      throw new IOException("Shard path [" + shardPath + "]");
    }
    FileStatus[] listStatus = fileSystem.listStatus(shardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".commit");          
      }
    });
    if (listStatus.length == 1) {
      FileStatus fs = listStatus[0];
      return fs.getPath();
    } else {
      throw new IOException("More than one sub dir [" + shardPath + "]");
    }
  }

  private boolean runAutomatic(String uuid, TableDescriptor descriptor, List<Path> inprogressPathList, String table,
      Path fileCache, Path outputPath, int reducerMultipler, Path tmpPath, TableStats tableStats, String snapshot)
      throws ClassNotFoundException, IOException, InterruptedException {
    PartitionedInputResult result = buildPartitionedInputData(uuid, tmpPath, descriptor, inprogressPathList, snapshot,
        fileCache);

    Job job = Job.getInstance(getConf(), "Blur Row Updater for table [" + table + "]");

    InputSplitPruneUtil.setBlurLookupRowIdFromNewDataCounts(job, table, result._rowIdsFromNewData);
    InputSplitPruneUtil.setBlurLookupRowIdUpdateFromNewDataCounts(job, table, result._rowIdsToUpdateFromNewData);
    InputSplitPruneUtil.setBlurLookupRowIdFromIndexCounts(job, table, result._rowIdsFromIndex);
    InputSplitPruneUtil.setTable(job, table);

    BlurInputFormat.setLocalCachePath(job, fileCache);

    // Existing data - This adds the copy data files first open and stream
    // through all documents.
    {
      Path tablePath = new Path(descriptor.getTableUri());
      BlurInputFormat.addTable(job, descriptor, MRUPDATE_SNAPSHOT);
      MultipleInputs.addInputPath(job, tablePath, PrunedBlurInputFormat.class, MapperForExistingDataMod.class);
    }

    // Existing data - This adds the row id lookup
    {
      MapperForExistingDataWithIndexLookup.setSnapshot(job, MRUPDATE_SNAPSHOT);
      FileInputFormat.addInputPath(job, result._partitionedInputData);
      MultipleInputs.addInputPath(job, result._partitionedInputData, PrunedSequenceFileInputFormat.class,
          MapperForExistingDataWithIndexLookup.class);
    }

    // New Data
    for (Path p : inprogressPathList) {
      FileInputFormat.addInputPath(job, p);
      MultipleInputs.addInputPath(job, p, SequenceFileInputFormat.class, MapperForNewDataMod.class);
    }

    BlurOutputFormat.setOutputPath(job, outputPath);
    BlurOutputFormat.setupJob(job, descriptor);

    job.setReducerClass(UpdateReducer.class);
    job.setMapOutputKeyClass(IndexKey.class);
    job.setMapOutputValueClass(IndexValue.class);
    job.setPartitionerClass(IndexKeyPartitioner.class);
    job.setGroupingComparatorClass(IndexKeyWritableComparator.class);

    BlurOutputFormat.setReducerMultiplier(job, reducerMultipler);

    boolean success = job.waitForCompletion(true);
    Counters counters = job.getCounters();
    LOG.info("Counters [" + counters + "]");
    return success;
  }

  private boolean runMrWithLookup(String uuid, TableDescriptor descriptor, List<Path> inprogressPathList, String table,
      Path fileCache, Path outputPath, int reducerMultipler, Path tmpPath, TableStats tableStats, String snapshot)
      throws ClassNotFoundException, IOException, InterruptedException {
    PartitionedInputResult result = buildPartitionedInputData(uuid, tmpPath, descriptor, inprogressPathList, snapshot,
        fileCache);

    Job job = Job.getInstance(getConf(), "Blur Row Updater for table [" + table + "]");

    MapperForExistingDataWithIndexLookup.setSnapshot(job, MRUPDATE_SNAPSHOT);
    FileInputFormat.addInputPath(job, result._partitionedInputData);
    MultipleInputs.addInputPath(job, result._partitionedInputData, SequenceFileInputFormat.class,
        MapperForExistingDataWithIndexLookup.class);

    for (Path p : inprogressPathList) {
      FileInputFormat.addInputPath(job, p);
      MultipleInputs.addInputPath(job, p, SequenceFileInputFormat.class, MapperForNewDataMod.class);
    }

    BlurOutputFormat.setOutputPath(job, outputPath);
    BlurOutputFormat.setupJob(job, descriptor);

    job.setReducerClass(UpdateReducer.class);
    job.setMapOutputKeyClass(IndexKey.class);
    job.setMapOutputValueClass(IndexValue.class);
    job.setPartitionerClass(IndexKeyPartitioner.class);
    job.setGroupingComparatorClass(IndexKeyWritableComparator.class);

    BlurOutputFormat.setReducerMultiplier(job, reducerMultipler);

    boolean success = job.waitForCompletion(true);
    Counters counters = job.getCounters();
    LOG.info("Counters [" + counters + "]");
    return success;
  }

  private boolean runMrOnly(TableDescriptor descriptor, List<Path> inprogressPathList, String table, Path fileCache,
      Path outputPath, int reducerMultipler) throws IOException, ClassNotFoundException, InterruptedException {
    Job job = Job.getInstance(getConf(), "Blur Row Updater for table [" + table + "]");
    Path tablePath = new Path(descriptor.getTableUri());
    BlurInputFormat.setLocalCachePath(job, fileCache);
    BlurInputFormat.addTable(job, descriptor, MRUPDATE_SNAPSHOT);
    MultipleInputs.addInputPath(job, tablePath, BlurInputFormat.class, MapperForExistingDataMod.class);

    for (Path p : inprogressPathList) {
      FileInputFormat.addInputPath(job, p);
      MultipleInputs.addInputPath(job, p, SequenceFileInputFormat.class, MapperForNewDataMod.class);
    }

    BlurOutputFormat.setOutputPath(job, outputPath);
    BlurOutputFormat.setupJob(job, descriptor);

    job.setReducerClass(UpdateReducer.class);
    job.setMapOutputKeyClass(IndexKey.class);
    job.setMapOutputValueClass(IndexValue.class);
    job.setPartitionerClass(IndexKeyPartitioner.class);
    job.setGroupingComparatorClass(IndexKeyWritableComparator.class);

    BlurOutputFormat.setReducerMultiplier(job, reducerMultipler);

    boolean success = job.waitForCompletion(true);
    Counters counters = job.getCounters();
    LOG.info("Counters [" + counters + "]");
    return success;
  }

  private PartitionedInputResult buildPartitionedInputData(String uuid, Path tmpPath, TableDescriptor descriptor,
      List<Path> inprogressPathList, String snapshot, Path fileCachePath) throws IOException, ClassNotFoundException,
      InterruptedException {
    Job job = Job.getInstance(getConf(), "Partitioning data for table [" + descriptor.getName() + "]");
    job.getConfiguration().set(BLUR_UPDATE_ID, uuid);

    // Needed for the bloom filter path information.
    BlurOutputFormat.setTableDescriptor(job, descriptor);
    BlurInputFormat.setLocalCachePath(job, fileCachePath);
    MapperForExistingDataWithIndexLookup.setSnapshot(job, snapshot);

    for (Path p : inprogressPathList) {
      FileInputFormat.addInputPath(job, p);
    }
    Path outputPath = new Path(tmpPath, UUID.randomUUID().toString());
    job.setJarByClass(getClass());
    job.setMapperClass(LookupBuilderMapper.class);
    job.setReducerClass(LookupBuilderReducer.class);

    int shardCount = descriptor.getShardCount();
    job.setNumReduceTasks(shardCount);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BooleanWritable.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    if (job.waitForCompletion(true)) {
      return new PartitionedInputResult(outputPath, job.getCounters(), shardCount, job.getTaskReports(TaskType.REDUCE));
    } else {
      throw new IOException("Partitioning failed!");
    }
  }

  private void waitForOtherSnapshotsToBeRemoved(Iface client, String table, String snapshot) throws BlurException,
      TException, InterruptedException {
    while (true) {
      Map<String, List<String>> listSnapshots = client.listSnapshots(table);
      boolean mrupdateSnapshots = false;
      for (Entry<String, List<String>> e : listSnapshots.entrySet()) {
        List<String> value = e.getValue();
        if (value.contains(snapshot)) {
          mrupdateSnapshots = true;
        }
      }
      if (!mrupdateSnapshots) {
        return;
      } else {
        LOG.info(snapshot + " Snapshot for table [{0}] already exists", table);
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        LOG.info("Retrying");
      }
    }
  }

  private List<Path> movePathList(FileSystem fileSystem, Path dstDir, List<Path> lst) throws IOException {
    List<Path> result = new ArrayList<Path>();
    for (Path src : lst) {
      Path dst = new Path(dstDir, src.getName());
      if (fileSystem.rename(src, dst)) {
        LOG.info("Moving [{0}] to [{1}]", src, dst);
        result.add(dst);
      } else {
        LOG.error("Could not move [{0}] to [{1}]", src, dst);
      }
    }
    return result;
  }

}
