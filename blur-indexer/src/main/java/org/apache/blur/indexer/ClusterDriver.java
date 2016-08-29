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
package org.apache.blur.indexer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurInputFormat;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ClusterDriver extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(ClusterDriver.class);
  private static final String BLUR_ENV = "blur.env";
  private static final String SEP = "_";
  private static final String IMPORT = "import";

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new ClusterDriver(), args));
  }

  @Override
  public int run(String[] args) throws Exception {
    int c = 0;
    final String blurEnv = args[c++];
    final String blurZkConnection = args[c++];
    final String extraConfig = args[c++];
    final int reducerMultiplier = 1;
    final Configuration conf = getConf();

    final ExecutorService service = Executors.newCachedThreadPool();
    final AtomicBoolean running = new AtomicBoolean();
    running.set(true);

    // Load configs for all filesystems.
    Path path = new Path(extraConfig);
    Configuration mergeHdfsConfigs = HdfsConfigurationNamespaceMerge.mergeHdfsConfigs(path.getFileSystem(conf), path);
    conf.addResource(mergeHdfsConfigs);
    conf.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, blurZkConnection);
    conf.set(BLUR_ENV, blurEnv);

    final Iface client = BlurClient.getClientFromZooKeeperConnectionStr(blurZkConnection);

    stopAllExistingMRJobs(blurEnv, conf);
    cleanUpOldImportDirs(client, conf);
    moveInprogressDirsBackToNew(client, conf);
    unlockLockedTables(client);

    Map<String, Future<Void>> futures = new HashMap<String, Future<Void>>();
    while (running.get()) {
      LOG.debug("Starting index update check for blur cluster [" + blurZkConnection + "].");
      try {
        List<String> tableList = client.tableList();
        startMissingIndexerThreads(tableList, service, futures, blurZkConnection, conf, client, reducerMultiplier);
      } catch (TException t) {
        LOG.error("Unknown Blur Thrift Error, Retrying...", t);
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
    }
    return 0;
  }

  private void unlockLockedTables(Iface client) throws BlurException, TException {
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      TableDescriptor tableDescriptor = client.describe(table);
      if (tableDescriptor.isEnabled()) {
        unlockLockedTables(client, table);
      }
    }
  }

  private void unlockLockedTables(Iface client, String table) throws BlurException, TException {
    Map<String, List<String>> listSnapshots = client.listSnapshots(table);
    for (Entry<String, List<String>> e : listSnapshots.entrySet()) {
      List<String> value = e.getValue();
      if (value.contains(IndexerJobDriver.MRUPDATE_SNAPSHOT)) {
        LOG.info("Unlocking table [{0}]", table);
        client.removeSnapshot(table, IndexerJobDriver.MRUPDATE_SNAPSHOT);
        return;
      }
    }
  }

  private void moveInprogressDirsBackToNew(Iface client, Configuration conf) throws BlurException, TException,
      IOException {
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      String mrIncWorkingPathStr = getMRIncWorkingPathStr(client, table);
      Path mrIncWorkingPath = new Path(mrIncWorkingPathStr);
      Path newData = new Path(mrIncWorkingPath, IndexerJobDriver.NEW);
      Path inprogressData = new Path(mrIncWorkingPath, IndexerJobDriver.INPROGRESS);
      FileSystem fileSystem = inprogressData.getFileSystem(conf);
      FileStatus[] listStatus = fileSystem.listStatus(inprogressData);
      for (FileStatus fileStatus : listStatus) {
        Path src = fileStatus.getPath();
        Path dst = new Path(newData, src.getName());
        if (fileSystem.rename(src, dst)) {
          LOG.info("Moved [{0}] to [{1}] to be reprocessed.", src, dst);
        } else {
          LOG.error("Could not move [{0}] to [{1}] to be reprocessed.", src, dst);
        }
      }
    }
  }

  private void cleanUpOldImportDirs(Iface client, Configuration conf) throws BlurException, TException, IOException {
    List<String> tableList = client.tableList();
    for (String table : tableList) {
      cleanUpOldImportDirs(client, conf, table);
    }
  }

  private void cleanUpOldImportDirs(Iface client, Configuration conf, String table) throws BlurException, TException,
      IOException {
    TableDescriptor descriptor = client.describe(table);
    String tableUri = descriptor.getTableUri();
    Path tablePath = new Path(tableUri);
    FileSystem fileSystem = tablePath.getFileSystem(getConf());
    Path importPath = new Path(tablePath, IMPORT);
    if (fileSystem.exists(importPath)) {
      for (FileStatus fileStatus : fileSystem.listStatus(importPath)) {
        Path path = fileStatus.getPath();
        LOG.info("Removing failed import [{0}]", path);
        fileSystem.delete(path, true);
      }
    }
  }

  private void stopAllExistingMRJobs(String blurEnv, Configuration conf) throws YarnException, IOException,
      InterruptedException {
    Cluster cluster = new Cluster(conf);
    JobStatus[] allJobStatuses = cluster.getAllJobStatuses();
    for (JobStatus jobStatus : allJobStatuses) {
      if (jobStatus.isJobComplete()) {
        continue;
      }
      String jobFile = jobStatus.getJobFile();
      JobID jobID = jobStatus.getJobID();
      Job job = cluster.getJob(jobID);
      FileSystem fileSystem = FileSystem.get(job.getConfiguration());
      Configuration configuration = new Configuration(false);
      Path path = new Path(jobFile);
      Path makeQualified = path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
      if (hasReadAccess(fileSystem, makeQualified)) {
        try (FSDataInputStream in = fileSystem.open(makeQualified)) {
          configuration.addResource(copy(in));
        }
        String jobBlurEnv = configuration.get(BLUR_ENV);
        LOG.info("Checking job [{0}] has env [{1}] current env set to [{2}]", jobID, jobBlurEnv, blurEnv);
        if (blurEnv.equals(jobBlurEnv)) {
          LOG.info("Killing running job [{0}]", jobID);
          job.killJob();
        }
      }
    }
  }

  private static InputStream copy(FSDataInputStream input) throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      IOUtils.copy(input, outputStream);
      return new ByteArrayInputStream(outputStream.toByteArray());
    }
  }

  private static boolean hasReadAccess(FileSystem fileSystem, Path p) {
    try {
      fileSystem.access(p, FsAction.READ);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private Callable<Void> getCallable(final String blurZkConnection, final Configuration conf, final Iface client,
      final String table, final int reducerMultiplier) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String originalThreadName = Thread.currentThread().getName();
        try {
          Thread.currentThread().setName(table);
          if (!isEnabled(client, table)) {
            LOG.info("Table [" + table + "] is not enabled.");
            return null;
          }
          waitForDataToLoad(client, table);
          LOG.debug("Starting index update for table [" + table + "].");
          final String mrIncWorkingPathStr = getMRIncWorkingPathStr(client, table);
          final String outputPathStr = getOutputPathStr(client, table);
          Path path = new Path(outputPathStr);
          FileSystem fileSystem = path.getFileSystem(getConf());

          Configuration configuration = new Configuration(conf);
          BlurInputFormat.setMaxNumberOfMaps(configuration, 10000);

          IndexerJobDriver driver = new IndexerJobDriver();
          driver.setConf(configuration);
          try {
            driver.run(new String[] { table, mrIncWorkingPathStr, outputPathStr, blurZkConnection,
                Integer.toString(reducerMultiplier) });
          } finally {
            if (fileSystem.exists(path)) {
              fileSystem.delete(path, true);
            }
          }
          return null;
        } finally {
          Thread.currentThread().setName(originalThreadName);
        }
      }
    };
  }

  private void startMissingIndexerThreads(List<String> tableList, ExecutorService service,
      Map<String, Future<Void>> futures, final String blurZkConnection, final Configuration conf, final Iface client,
      int reducerMultiplier) throws BlurException, TException {
    Set<String> tables = new HashSet<String>(tableList);

    // remove futures that are complete
    for (String table : tables) {
      Future<Void> future = futures.get(table);
      if (future != null) {
        if (future.isDone()) {
          try {
            future.get();
          } catch (InterruptedException e) {
            LOG.error("Unknown error while processing table [" + table + "].", e);
          } catch (ExecutionException e) {
            LOG.error("Unknown error while processing table [" + table + "].", e.getCause());
          }
          futures.remove(table);
        } else {
          LOG.info("Update for table [" + table + "] still running.");
        }
      }
    }

    // start missing tables
    for (String table : tables) {
      if (!futures.containsKey(table)) {
        if (isEnabled(client, table)) {
          Future<Void> future = service.submit(getCallable(blurZkConnection, conf, client, table, reducerMultiplier));
          futures.put(table, future);
        }
      }
    }
  }

  public static void waitForDataToLoad(Iface client, String table) throws BlurException, TException,
      InterruptedException {
    if (isFullyLoaded(client.tableStats(table))) {
      return;
    }
    while (true) {
      TableStats tableStats = client.tableStats(table);
      if (isFullyLoaded(tableStats)) {
        LOG.info("Data load complete in table [" + table + "] [" + tableStats + "]");
        return;
      }
      LOG.info("Waiting for data to load in table [" + table + "] [" + tableStats + "]");
      Thread.sleep(5000);
    }
  }

  private static boolean isFullyLoaded(TableStats tableStats) {
    if (tableStats.getSegmentImportInProgressCount() == 0 && tableStats.getSegmentImportPendingCount() == 0) {
      return true;
    }
    return false;
  }

  private boolean isEnabled(Iface client, String table) throws BlurException, TException {
    TableDescriptor tableDescriptor = client.describe(table);
    return tableDescriptor.isEnabled();
  }

  private void mkdirs(FileSystem fileSystem, Path path) throws IOException {
    if (fileSystem.exists(path)) {
      return;
    }
    LOG.info("Creating path [" + path + "].");
    if (!fileSystem.mkdirs(path)) {
      LOG.error("Path [" + path + "] could not be created.");
    }
  }

  public static String getMRIncWorkingPathStr(Iface client, String table) throws BlurException, TException, IOException {
    TableDescriptor descriptor = client.describe(table);
    Map<String, String> tableProperties = descriptor.getTableProperties();
    if (tableProperties != null) {
      String workingPath = tableProperties.get(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH);
      if (workingPath != null) {
        return workingPath;
      }
    }
    throw new IOException("Table [" + table + "] does not have the property ["
        + BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH + "] setup correctly.");
  }

  private String getOutputPathStr(Iface client, String table) throws BlurException, TException, IOException {
    TableDescriptor descriptor = client.describe(table);
    String tableUri = descriptor.getTableUri();
    Path tablePath = new Path(tableUri);
    FileSystem fileSystem = tablePath.getFileSystem(getConf());
    Path importPath = new Path(tablePath, IMPORT);
    mkdirs(fileSystem, importPath);
    return new Path(importPath, IMPORT + SEP + System.currentTimeMillis() + SEP + UUID.randomUUID().toString())
        .toString();
  }

}