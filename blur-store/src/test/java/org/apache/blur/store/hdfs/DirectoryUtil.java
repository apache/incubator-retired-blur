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
package org.apache.blur.store.hdfs;

import java.io.IOException;
import java.util.Timer;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.hdfs_v2.FastHdfsKeyValueDirectory;
import org.apache.blur.store.hdfs_v2.JoinDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;

public class DirectoryUtil {

  private static final Log LOG = LogFactory.getLog(DirectoryUtil.class);

  public static Directory getDirectory(Configuration configuration, HdfsDirectory dir, boolean disableFast,
      Timer hdfsKeyValueTimer, String table, String shard, boolean readOnly) throws IOException {
    Path hdfsDirPath = dir.getPath();
    String scheme = hdfsDirPath.toUri().getScheme();
    if (scheme != null && scheme.equals("hdfs") && !disableFast) {
      LOG.info("Using Fast HDFS directory implementation on shard [{0}] for table [{1}]", shard, table);
      FastHdfsKeyValueDirectory shortTermStorage = new FastHdfsKeyValueDirectory(readOnly, hdfsKeyValueTimer,
          configuration, getFastDirectoryPath(hdfsDirPath));
      return new JoinDirectory(dir, shortTermStorage);
    } else {
      LOG.info("Using regular HDFS directory.");
      return dir;
    }
  }

  public static Path getFastDirectoryPath(Path hdfsDirPath) {
    return new Path(hdfsDirPath, "fast");
  }

}
