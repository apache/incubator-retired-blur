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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

  public static final String MRUPDATE_SNAPSHOT = "mrupdate-snapshot";
  public static final String CACHE = "cache";
  public static final String COMPLETE = "complete";
  public static final String INPROGRESS = "inprogress";
  public static final String NEW = "new";
  private static final Log LOG = LogFactory.getLog(Driver.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Driver(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    int c = 0;
    if (args.length < 5) {
      System.err
          .println("Usage Driver <table> <mr inc working path> <output path> <zk connection> <reducer multipler> <extra config files...>");
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
    try {
      inprogressPathList = movePathList(fileSystem, inprogressData, srcPathList);

      Job job = Job.getInstance(getConf(), "Blur Row Updater for table [" + table + "]");
      client = BlurClient.getClientFromZooKeeperConnectionStr(blurZkConnection);
      waitForOtherSnapshotsToBeRemoved(client, table, MRUPDATE_SNAPSHOT);
      client.createSnapshot(table, MRUPDATE_SNAPSHOT);
      TableDescriptor descriptor = client.describe(table);
      Path tablePath = new Path(descriptor.getTableUri());

      BlurInputFormat.setLocalCachePath(job, fileCache);
      BlurInputFormat.addTable(job, descriptor, MRUPDATE_SNAPSHOT);
      MultipleInputs.addInputPath(job, tablePath, BlurInputFormat.class, MapperForExistingData.class);
      for (Path p : inprogressPathList) {
        FileInputFormat.addInputPath(job, p);
        MultipleInputs.addInputPath(job, p, SequenceFileInputFormat.class, MapperForNewData.class);
      }

      BlurOutputFormat.setOutputPath(job, outputPath);
      BlurOutputFormat.setupJob(job, descriptor);

      job.setReducerClass(UpdateReducer.class);
      job.setMapOutputKeyClass(IndexKey.class);
      job.setMapOutputValueClass(IndexValue.class);
      job.setPartitionerClass(IndexKeyPartitioner.class);
      job.setGroupingComparatorClass(IndexKeyWritableComparator.class);

      BlurOutputFormat.setReducerMultiplier(job, reducerMultipler);

      success = job.waitForCompletion(true);
      Counters counters = job.getCounters();
      LOG.info("Counters [" + counters + "]");

    } finally {
      if (success) {
        LOG.info("Indexing job succeeded!");
        movePathList(fileSystem, completeData, inprogressPathList);
      } else {
        LOG.error("Indexing job failed!");
        movePathList(fileSystem, newData, inprogressPathList);
      }
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
