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
package org.apache.blur.store.hdfs_v2;

import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {

  private static final String IN = "in";
  private static final String GET_FILE_LENGTH = "getFileLength";

  public static long getFileLength(FileSystem fileSystem, Path path, FSDataInputStream inputStream) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    long dfsLength = getDFSLength(inputStream);
    return Math.max(dfsLength, fileStatus.getLen());
  }

  public static long getDFSLength(FSDataInputStream inputStream) throws IOException {
    try {
      Field field = FilterInputStream.class.getDeclaredField(IN);
      field.setAccessible(true);
      Object dfs = field.get(inputStream);
      Method method = dfs.getClass().getMethod(GET_FILE_LENGTH, new Class[] {});
      Object length = method.invoke(dfs, new Object[] {});
      return (Long) length;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
