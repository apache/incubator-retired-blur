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
package org.apache.blur.command;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.commandtype.ClusterExecuteCommandSingleTable;
import org.apache.blur.mapreduce.lib.update.Driver;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class BulkTableUpdate extends ClusterExecuteCommandSingleTable<Integer> {

  private static final String YARN_SITE_XML = "yarn-site.xml";
  private static final String HDFS_SITE_XML = "hdfs-site.xml";
  private static final String BLUR_BULK_UPDATE_WORKING_PATH = "blur.bulk.update.working.path";
  private static final String IMPORT = "import";
  private static final String BULK_UPDATE = "bulk-update";

  @OptionalArgument("The reducer multipler.")
  private Integer reducerMultipler = 1;

  @OptionalArgument("Automatically load the indexed data into the table when complete.")
  private Boolean autoLoad = true;

  @OptionalArgument("Block and wait for import to complete.")
  private boolean waitForDataBeVisible = true;

  @OptionalArgument("Additional configurations that may needed to execute the indexing job.")
  private List<String> extraConfigs = new ArrayList<String>();

  @Override
  public String getName() {
    return BULK_UPDATE;
  }

  @Override
  public Integer clusterExecute(ClusterContext context) throws IOException, InterruptedException {
    String table = getTable();
    BlurConfiguration blurConfiguration = context.getBlurConfiguration(table);
    String blurZkConnection = blurConfiguration.get(BlurConstants.BLUR_ZOOKEEPER_CONNECTION);
    TableContext tableContext = context.getTableContext(table);
    TableDescriptor descriptor = tableContext.getDescriptor();
    String tableUri = descriptor.getTableUri();
    String mrIncWorkingPathStr = blurConfiguration.get(BLUR_BULK_UPDATE_WORKING_PATH);
    Path mrIncWorkingPath = new Path(mrIncWorkingPathStr, table);
    String outputPathStr = new Path(new Path(tableUri), IMPORT).toString();
    Configuration configuration = new Configuration();
    configuration.addResource(HDFS_SITE_XML);
    configuration.addResource(YARN_SITE_XML);
    for (String s : extraConfigs) {
      if (s != null) {
        InputStream inputStream = IOUtils.toInputStream(s);
        configuration.addResource(inputStream);
        inputStream.close();
      }
    }
    int run;
    try {
      run = ToolRunner.run(configuration, new Driver(), new String[] { table, mrIncWorkingPath.toString(),
          outputPathStr, blurZkConnection, Integer.toString(reducerMultipler) });
    } catch (Exception e) {
      throw new IOException(e);
    }
    if (run == 0 && autoLoad) {
      Iface client = BlurClient.getClient();
      try {
        client.loadData(table, outputPathStr);
        if (waitForDataBeVisible) {
          waitForDataToBeVisible(client, table);
        }
      } catch (BlurException e) {
        throw new IOException(e);
      } catch (TException e) {
        throw new IOException(e);
      }
    }
    return run;
  }

  private void waitForDataToBeVisible(Iface client, String table) throws BlurException, TException,
      InterruptedException {
    while (true) {
      TableStats tableStats = client.tableStats(table);
      if (tableStats.getSegmentImportInProgressCount() > 0) {
        break;
      } else if (tableStats.getSegmentImportPendingCount() > 0) {
        break;
      }
      Thread.sleep(1000);
    }

    // Once 0 is met wait for 5 more seconds, just in case there is a slow shard
    // server.
    for (int i = 0; i < 5; i++) {
      INNER: while (true) {
        TableStats tableStats = client.tableStats(table);
        if (tableStats.getSegmentImportInProgressCount() == 0) {
          break INNER;
        } else if (tableStats.getSegmentImportPendingCount() == 0) {
          break INNER;
        }
        Thread.sleep(1000);
      }
    }
  }

  public Integer getReducerMultipler() {
    return reducerMultipler;
  }

  public void setReducerMultipler(Integer reducerMultipler) {
    this.reducerMultipler = reducerMultipler;
  }

  public Boolean getAutoLoad() {
    return autoLoad;
  }

  public void setAutoLoad(Boolean autoLoad) {
    this.autoLoad = autoLoad;
  }

  public boolean isWaitForDataBeVisible() {
    return waitForDataBeVisible;
  }

  public void setWaitForDataBeVisible(boolean waitForDataBeVisible) {
    this.waitForDataBeVisible = waitForDataBeVisible;
  }

  public List<String> getExtraConfigs() {
    return extraConfigs;
  }

  public void setExtraConfigs(List<String> extraConfigs) {
    this.extraConfigs = extraConfigs;
  }

  public void addExtraConfig(Configuration configuration) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    configuration.writeXml(outputStream);
    outputStream.close();
    extraConfigs.add(new String(outputStream.toByteArray()));
  }

}
