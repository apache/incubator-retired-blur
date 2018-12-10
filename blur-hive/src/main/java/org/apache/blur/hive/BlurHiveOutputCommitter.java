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

import static org.apache.blur.hive.BlurSerDe.BLUR_BLOCKING_APPLY;
import static org.apache.blur.hive.BlurSerDe.BLUR_CONTROLLER_CONNECTION_STR;

import java.io.IOException;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class BlurHiveOutputCommitter extends OutputCommitter {

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

  private void finishBulkJob(JobContext context, boolean apply) throws IOException {
    Configuration configuration = context.getConfiguration();
    String connectionStr = configuration.get(BLUR_CONTROLLER_CONNECTION_STR);
    boolean blocking = configuration.getBoolean(BLUR_BLOCKING_APPLY, false);
    Iface client = BlurClient.getClient(connectionStr);
    String bulkId = BlurHiveOutputFormat.getBulkId(configuration);
    try {
      client.bulkMutateFinish(bulkId, apply, blocking);
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
