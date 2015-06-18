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
package org.apache.blur.hive;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.update.BulkTableUpdateCommand;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;

public class BlurHiveMRLoaderOutputCommitter extends OutputCommitter {

  public static final String YARN_SITE_XML = "yarn-site.xml";
  public static final String MAPRED_SITE_XML = "mapred-site.xml";
  public static final String HDFS_SITE_XML = "hdfs-site.xml";

  private static final Log LOG = LogFactory.getLog(BlurHiveMRLoaderOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public void abortJob(JobContext context, int status) throws IOException {
    finishBulkJob(context, false);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {

  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    finishBulkJob(context, true);
  }

  private void finishBulkJob(JobContext context, final boolean apply) throws IOException {
    final Configuration configuration = context.getConfiguration();
    PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        String workingPathStr = configuration.get(BlurConstants.BLUR_BULK_UPDATE_WORKING_PATH);
        Path workingPath = new Path(workingPathStr);
        Path tmpDir = new Path(workingPath, "tmp");
        FileSystem fileSystem = tmpDir.getFileSystem(configuration);
        String loadId = configuration.get(BlurSerDe.BLUR_MR_LOAD_ID);
        Path loadPath = new Path(tmpDir, loadId);

        if (apply) {
          Path newDataPath = new Path(workingPath, "new");
          Path dst = new Path(newDataPath, loadId);
          if (!fileSystem.rename(loadPath, dst)) {
            LOG.error("Could not move data from src [" + loadPath + "] to dst [" + dst + "]");
            throw new IOException("Could not move data from src [" + loadPath + "] to dst [" + dst + "]");
          }

          TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
          String connectionStr = configuration.get(BlurSerDe.BLUR_CONTROLLER_CONNECTION_STR);
          BulkTableUpdateCommand bulkTableUpdateCommand = new BulkTableUpdateCommand();
          bulkTableUpdateCommand.setAutoLoad(true);
          bulkTableUpdateCommand.setTable(tableDescriptor.getName());
          bulkTableUpdateCommand.setWaitForDataBeVisible(true);

          Configuration config = new Configuration(false);
          config.addResource(HDFS_SITE_XML);
          config.addResource(YARN_SITE_XML);
          config.addResource(MAPRED_SITE_XML);

          bulkTableUpdateCommand.addExtraConfig(config);
          if (bulkTableUpdateCommand.run(BlurClient.getClient(connectionStr)) != 0) {
            throw new IOException("Unknown error occured duing load.");
          }
        } else {
          fileSystem.delete(loadPath, true);
        }
        return null;
      }
    };
    UserGroupInformation userGroupInformation = BlurHiveOutputFormat.getUGI(configuration);
    try {
      userGroupInformation.doAs(action);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
