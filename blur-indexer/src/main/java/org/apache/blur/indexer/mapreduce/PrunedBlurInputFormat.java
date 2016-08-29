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
package org.apache.blur.indexer.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.blur.indexer.InputSplitPruneUtil;
import org.apache.blur.mapreduce.lib.BlurInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class PrunedBlurInputFormat extends BlurInputFormat {

  private static final Log LOG = LogFactory.getLog(PrunedBlurInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Path[] dirs = getInputPaths(context);
    Configuration configuration = context.getConfiguration();
    List<BlurInputSplit> splits = getSplits(configuration, dirs);
    Map<Path, List<BlurInputSplit>> splitMap = new TreeMap<Path, List<BlurInputSplit>>();
    for (BlurInputSplit split : splits) {
      Path path = split.getDir();
      String table = split.getTable().toString();
      int shard = InputSplitPruneUtil.getShardFromDirectoryPath(path);
      long rowIdUpdateFromNewDataCount = InputSplitPruneUtil.getBlurLookupRowIdUpdateFromNewDataCount(configuration,
          table, shard);
      long indexCount = InputSplitPruneUtil.getBlurLookupRowIdFromIndexCount(configuration, table, shard);
      if (rowIdUpdateFromNewDataCount == 0 || indexCount == 0) {
        LOG.debug("Pruning id lookup input path [" + path + "] no overlapping ids.");
      } else if (InputSplitPruneUtil.shouldLookupExecuteOnShard(configuration, table, shard)) {
        LOG.debug("Pruning blur input path [" + split.getDir() + "]");
      } else {
        LOG.debug("Keeping blur input path [" + split.getDir() + "]");
        List<BlurInputSplit> list = splitMap.get(path);
        if (list == null) {
          splitMap.put(path, list = new ArrayList<BlurInputSplit>());
        }
        list.add(split);
      }
    }
    List<InputSplit> result = new ArrayList<InputSplit>();
    for (List<BlurInputSplit> lst : splitMap.values()) {
      BlurInputSplitColletion blurInputSplitColletion = new BlurInputSplitColletion();
      for (BlurInputSplit blurInputSplit : lst) {
        blurInputSplitColletion.add(blurInputSplit);
      }
      result.add(blurInputSplitColletion);
    }
    return result;
  }
}
