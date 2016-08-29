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

import org.apache.blur.indexer.InputSplitPruneUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.google.common.base.Splitter;

public class PrunedSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(PrunedSequenceFileInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    List<InputSplit> results = new ArrayList<InputSplit>();
    Configuration configuration = job.getConfiguration();
    String table = InputSplitPruneUtil.getTable(configuration);
    for (InputSplit inputSplit : splits) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      Path path = fileSplit.getPath();
      LOG.debug("Getting shard index from path [" + path + "]");
      String name = path.getName();
      int shard = getShardIndex(name);
      long rowIdUpdateFromNewDataCount = InputSplitPruneUtil.getBlurLookupRowIdUpdateFromNewDataCount(configuration,
          table, shard);
      long indexCount = InputSplitPruneUtil.getBlurLookupRowIdFromIndexCount(configuration, table, shard);
      if (rowIdUpdateFromNewDataCount == 0 || indexCount == 0) {
        LOG.debug("Pruning id lookup input path [" + path + "] no overlapping ids.");
      } else if (InputSplitPruneUtil.shouldLookupExecuteOnShard(configuration, table, shard)) {
        LOG.debug("Keeping id lookup input path [" + path + "]");
        results.add(inputSplit);
      } else {
        LOG.debug("Pruning id lookup input path [" + path + "]");
      }
    }
    return results;
  }

  private int getShardIndex(String name) {
    // based on file format of "part-r-00000", etc
    Iterable<String> split = Splitter.on('-').split(name);
    List<String> parts = new ArrayList<String>();
    for (String s : split) {
      parts.add(s);
    }
    return Integer.parseInt(parts.get(2));
  }

}
