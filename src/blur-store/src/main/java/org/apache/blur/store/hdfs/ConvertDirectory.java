package org.apache.blur.store.hdfs;

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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

public class ConvertDirectory {

  public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
    Path path = new Path(args[0]);
    convert(path);
  }

  public static void convert(Path path) throws IOException {
    FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
    if (!fileSystem.exists(path)) {
      System.out.println(path + " does not exists.");
      return;
    }
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (fileStatus.isDir()) {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus status : listStatus) {
        convert(status.getPath());
      }
    } else {
      System.out.println("Converting file [" + path + "]");
      HdfsMetaBlock block = new HdfsMetaBlock();
      block.realPosition = 0;
      block.logicalPosition = 0;
      block.length = fileStatus.getLen();
      FSDataOutputStream outputStream = fileSystem.append(path);
      block.write(outputStream);
      outputStream.writeInt(1);
      outputStream.writeLong(fileStatus.getLen());
      outputStream.writeInt(HdfsFileWriter.VERSION);
      outputStream.close();
    }
  }
}
