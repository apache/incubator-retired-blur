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

import java.io.IOException;

import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class CheckOutputSpecs {
  
  public static void checkOutputSpecs(Configuration config, int reducers) throws IOException, InterruptedException {
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(config);
    if (tableDescriptor == null) {
      throw new IOException("setTableDescriptor needs to be called first.");
    }
    Path outputPath = BlurOutputFormat.getOutputPath(config);
    if (outputPath == null) {
      throw new IOException("Output path is not set.");
    }
    BlurUtil.validateWritableDirectory(outputPath.getFileSystem(config), outputPath);
    int shardCount = tableDescriptor.getShardCount();
    int reducerMultiplier = BlurOutputFormat.getReducerMultiplier(config);
    int validNumberOfReducers = reducerMultiplier * shardCount;
    if (reducers > 0 && reducers != validNumberOfReducers) {
      throw new IllegalArgumentException("Invalid number of reducers [ " + reducers + " ]."
          + " Number of Reducers should be [ " + validNumberOfReducers + " ].");
    }
  }
  
}
