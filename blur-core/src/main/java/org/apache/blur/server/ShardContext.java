package org.apache.blur.server;

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
import java.io.IOException;

import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;

public class ShardContext {

  private String shard;
  private Path hdfsDirPath;
  private Directory directory;
  private TableContext tableContext;

  public TableContext getTableContext() {
    return tableContext;
  }

  public void setTableContext(TableContext tableContext) {
    this.tableContext = tableContext;
  }

  protected ShardContext() {

  }

  public Directory getDirectory() {
    return directory;
  }

  public void setDirectory(Directory directory) {
    this.directory = directory;
  }

  public Path getHdfsDirPath() {
    return hdfsDirPath;
  }

  public void setHdfsDirPath(Path hdfsDirPath) {
    this.hdfsDirPath = hdfsDirPath;
  }

  public String getShard() {
    return shard;
  }

  public void setShard(String shard) {
    this.shard = shard;
  }

  public static ShardContext create(TableContext tableContext, String shard) throws IOException {
    ShardUtil.validateShardName(shard);
    ShardContext shardContext = new ShardContext();
    shardContext.tableContext = tableContext;
    shardContext.hdfsDirPath = new Path(tableContext.getTablePath(), shard);
    shardContext.shard = shard;
    shardContext.directory = new HdfsDirectory(tableContext.getConfiguration(), shardContext.hdfsDirPath);
    return shardContext;
  }

}
