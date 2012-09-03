package com.nearinfinity.blur.store.hdfs;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ChangeFileExt {

  public static void main(String[] args) throws IOException {
    Path p = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(p.toUri(), new Configuration());
    FileStatus[] listStatus = fileSystem.listStatus(p);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      fileSystem.rename(path, new Path(path.toString() + ".lf"));
    }
  }

}
