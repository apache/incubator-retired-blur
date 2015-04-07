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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

public class HdfsDirectoryCopyFileTest {

  private Path _base;
  private Configuration _configuration;
  private FileSystem _fileSystem;

  @Before
  public void setup() throws IOException {
    _base = new Path("./target/tmp/HdfsDirectoryTest");
    _configuration = new Configuration();

    _fileSystem = _base.getFileSystem(_configuration);
    _fileSystem.delete(_base, true);
    _fileSystem.mkdirs(_base);
  }

  @Test
  public void testFileCopyTest() throws IOException, InterruptedException {
    HdfsDirectory dir1 = new HdfsDirectory(_configuration, new Path(_base, "dir1"));
    String name = "file1.txt";
    IndexOutput out = dir1.createOutput(name, IOContext.DEFAULT);
    out.writeLong(0L);
    out.close();

    long fileModified1 = dir1.getFileModified(name);
    long fileLength1 = dir1.fileLength(name);
    String[] listAll1 = dir1.listAll();

    Thread.sleep(100);
    dir1.runHdfsCopyFile(name);

    Path path = dir1.getPath();
    FileSystem fileSystem = path.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(path);
    for (FileStatus status : listStatus) {
      System.out.println(status.getPath());
    }

    {
      long fileModified2 = dir1.getFileModified(name);
      long fileLength2 = dir1.fileLength(name);
      String[] listAll2 = dir1.listAll();

      assertEquals(fileLength1, fileLength2);
      assertEquals(fileModified1, fileModified2);
      assertTrue(Arrays.equals(listAll1, listAll2));
    }

    dir1.close();

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, new Path(_base, "dir1"));
    {
      long fileModified2 = dir2.getFileModified(name);
      long fileLength2 = dir2.fileLength(name);
      String[] listAll2 = dir2.listAll();

      assertEquals(fileLength1, fileLength2);
      assertEquals(fileModified1, fileModified2);
      assertTrue(Arrays.equals(listAll1, listAll2));
    }

    dir2.close();

  }

}
