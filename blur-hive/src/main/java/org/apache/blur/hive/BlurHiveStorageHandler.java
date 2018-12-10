/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.util.Map;
import java.util.UUID;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class BlurHiveStorageHandler extends DefaultStorageHandler {

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return NullHiveInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return BlurHiveOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return BlurSerDe.class;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    if (BlurSerDe.shouldUseMRWorkingPath(jobConf)) {
      String loadId = UUID.randomUUID().toString();
      jobConf.set(BlurSerDe.BLUR_MR_LOAD_ID, loadId);
      jobConf.setOutputCommitter(BlurHiveMRLoaderOutputCommitter.class);
    } else {
      try {
        String bulkId = UUID.randomUUID().toString();
        String connectionStr = jobConf.get(BlurSerDe.BLUR_CONTROLLER_CONNECTION_STR);
        Iface client = BlurClient.getClient(connectionStr);
        client.bulkMutateStart(bulkId);
        BlurHiveOutputFormat.setBulkId(jobConf, bulkId);
        jobConf.setOutputCommitter(BlurHiveOutputCommitter.class);
      } catch (BlurException e) {
        throw new RuntimeException(e);
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

}
