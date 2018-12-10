package org.apache.blur.utils;

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
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.blur.lucene.codec.Blur024Codec;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

/**
 * This class is used to reduce the total number of shards of a table. The main
 * use would be if during an indexing job the number of reducers were increased
 * to make indexing faster, but the total number of shards in the table needed
 * to be smaller. This utility safely collapses indexes together thus reducing
 * the total number of shards in the table.
 * 
 * For example if you wanted to run 1024 reducers but only wanted to run 128
 * shards in a table. After the bulk map reducer job finishes, this utility
 * could be executed:
 * 
 * TableShardCountCollapser <hdfs path> 128
 * 
 * The result would be 128 shards in the table path.
 * 
 */
public class TableShardCountCollapser extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Let ToolRunner handle generic command-line options
    int res = ToolRunner.run(new Configuration(), new TableShardCountCollapser(), args);
    System.exit(res);
  }

  private Path path;

  @Override
  public int run(String[] args) throws Exception {
    // prompt to make sure the table is not enabled

    Path path = new Path(args[0]);
    int count = Integer.parseInt(args[1]);
    setTablePath(path);
    collapseShardsTo(count);
    return 0;
  }

  public boolean validateCount(int count) throws IOException {
    if (getCollapsePossibilities().contains(count)) {
      return true;
    }
    return false;
  }

  public void setTablePath(Path path) {
    this.path = path;
  }

  public List<Integer> getCollapsePossibilities() throws IOException {
    FileSystem fileSystem = path.getFileSystem(getConf());
    FileStatus[] listStatus = fileSystem.listStatus(path);
    SortedSet<String> shards = new TreeSet<String>();
    for (FileStatus status : listStatus) {
      Path shardPath = status.getPath();
      if (shardPath.getName().startsWith(BlurConstants.SHARD_PREFIX)) {
        shards.add(shardPath.getName());
      }
    }
    validateShards(shards);
    List<Integer> result = getFactors(shards.size());
    return result;
  }

  private List<Integer> getFactors(int size) {
    List<Integer> result = new ArrayList<Integer>();
    for (int i = 1; i < size; i++) {
      if (size % i == 0) {
        result.add(i);
      }
    }
    return result;
  }

  private void validateShards(SortedSet<String> shards) {
    int count = shards.size();
    for (int i = 0; i < count; i++) {
      if (!shards.contains(ShardUtil.getShardName(i))) {
        throw new RuntimeException("Invalid table");
      }
    }
  }

  public void collapseShardsTo(int newShardCount) throws IOException {
    if (!validateCount(newShardCount)) {
      throw new RuntimeException("Count [" + newShardCount + "] is not valid, valid values are ["
          + getCollapsePossibilities() + "]");
    }

    Path[] paths = getPaths();
    int numberOfShardsToMergePerPass = paths.length / newShardCount;
    for (int i = 0; i < newShardCount; i++) {
      System.out.println("Base Index [" + paths[i] + "]");
      IndexWriterConfig lconf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
      lconf.setCodec(new Blur024Codec());
      HdfsDirectory dir = new HdfsDirectory(getConf(), paths[i]);
      IndexWriter indexWriter = new IndexWriter(dir, lconf);
      Directory[] dirs = new Directory[numberOfShardsToMergePerPass - 1];
      Path[] pathsToDelete = new Path[numberOfShardsToMergePerPass - 1];
      for (int p = 1; p < numberOfShardsToMergePerPass; p++) {
        Path pathToMerge = paths[i + p * newShardCount];
        System.out.println("Merge [" + pathToMerge + "]");
        dirs[p - 1] = new HdfsDirectory(getConf(), pathToMerge);
        pathsToDelete[p - 1] = pathToMerge;
      }
      indexWriter.addIndexes(dirs);
      // Causes rewrite of of index and the symlinked files are
      // merged/rewritten.
      indexWriter.forceMerge(1);
      indexWriter.close();
      FileSystem fileSystem = path.getFileSystem(getConf());
      for (Path p : pathsToDelete) {
        fileSystem.delete(p, true);
      }
    }
  }

  private Path[] getPaths() throws IOException {
    FileSystem fileSystem = path.getFileSystem(getConf());
    FileStatus[] listStatus = fileSystem.listStatus(path);
    SortedSet<Path> shards = new TreeSet<Path>();
    for (FileStatus status : listStatus) {
      Path shardPath = status.getPath();
      if (shardPath.getName().startsWith(BlurConstants.SHARD_PREFIX)) {
        shards.add(shardPath);
      }
    }
    return shards.toArray(new Path[shards.size()]);
  }

}
