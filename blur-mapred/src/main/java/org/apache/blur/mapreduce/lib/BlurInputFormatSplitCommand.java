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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.blur.command.BlurArray;
import org.apache.blur.command.BlurObject;
import org.apache.blur.command.CombiningContext;
import org.apache.blur.command.IndexContext;
import org.apache.blur.command.Location;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.command.commandtype.ClusterServerReadCommandSingleTable;
import org.apache.blur.mapreduce.lib.BlurInputFormat.BlurInputSplit;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

public class BlurInputFormatSplitCommand extends ClusterServerReadCommandSingleTable<BlurObject> {

  @RequiredArgument
  private String snapshot;

  @Override
  public BlurObject execute(IndexContext context) throws IOException, InterruptedException {
    String shard = context.getShard().getShard();
    TableContext tableContext = context.getTableContext();
    ShardContext shardContext = ShardContext.create(tableContext, shard);

    Path shardDir = shardContext.getHdfsDirPath();
    Configuration configuration = tableContext.getConfiguration();
    String table = tableContext.getTable();
    Directory directory = getDirectory(context.getIndexReader());

    List<BlurInputSplit> list = BlurInputFormat.getSplitForDirectory(shardDir, configuration, table, snapshot,
        directory);
    BlurArray splits = BlurInputFormat.toBlurArray(list);
    BlurObject blurObject = new BlurObject();
    blurObject.put(shard, splits);
    return blurObject;
  }

  private Directory getDirectory(IndexReader indexReader) {
    DirectoryReader directoryReader = (DirectoryReader) indexReader;
    return directoryReader.directory();
  }

  @Override
  public BlurObject combine(CombiningContext context, Map<? extends Location<?>, BlurObject> results)
      throws IOException, InterruptedException {
    BlurObject blurObject = new BlurObject();
    for (BlurObject shardSplits : results.values()) {
      Iterator<String> keys = shardSplits.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        blurObject.put(key, shardSplits.getBlurArray(key));
      }
    }
    return blurObject;
  }

  @Override
  public String getName() {
    return "input-format-split";
  }

  public String getSnapshot() {
    return snapshot;
  }

  public void setSnapshot(String snapshot) {
    this.snapshot = snapshot;
  }

}
