package org.apache.blur.store.hdfs;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class HdfsDirectoryManifestFileCacheTest {

  private Configuration _configuration = new Configuration();
  private Path _root;
  private FileSystem _fileSystem;

  @Before
  public void setup() throws IOException {
    _root = new Path(new File("target/tmp/HdfsDirectoryFileCacheTest").getAbsolutePath());
    _fileSystem = _root.getFileSystem(_configuration);
    _fileSystem.delete(_root, true);
  }

  @After
  public void teardown() throws IOException {
    _fileSystem.delete(_root, true);
  }

  @Test
  public void test1() throws IOException {
    Path path = new Path(_root, "dir1");
    HdfsDirectory dir = new HdfsDirectory(_configuration, path);
    createFiles(_configuration, 10, 10, path, dir);
    dir.close();

    _fileSystem.delete(new Path(path, "file_manifest"), false);
    long t1 = System.nanoTime();
    // Rebuilds manifest
    new HdfsDirectory(_configuration, path).close();
    long t2 = System.nanoTime();
    // Uses manifest
    new HdfsDirectory(_configuration, path).close();
    long t3 = System.nanoTime();
    System.out.println("No manifest [" + (t2 - t1) / 1000000.0 + "]");
    System.out.println("With manifest [" + (t3 - t2) / 1000000.0 + "]");
  }

  @Test
  public void test2() throws IOException {
    Path path = new Path(_root, "dir2");
    {
      HdfsDirectory dir1 = new HdfsDirectory(_configuration, path);
      createFile(dir1, "file1");
      createFile(dir1, "file2");
      createFile(dir1, "file3");
      dir1.close();
    }

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, path);
    String[] listAll = dir2.listAll();
    assertEquals(3, listAll.length);
    assertEquals(new HashSet<String>(Arrays.asList("file1", "file2", "file3")),
        new HashSet<String>(Arrays.asList(listAll)));
    for (String f : listAll) {
      assertEquals(4, dir2.fileLength(f));
    }
    dir2.close();
  }

  @Test
  public void test3() throws IOException {
    Path path = new Path(_root, "dir3");
    {
      HdfsDirectory dir1 = new HdfsDirectory(_configuration, path);
      createFile(dir1, "file1");
      createFile(dir1, "file2");
      createFile(dir1, "file3");
      dir1.deleteFile("file3");
      dir1.close();
    }

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, path);
    String[] listAll = dir2.listAll();
    assertEquals(2, listAll.length);
    assertEquals(new HashSet<String>(Arrays.asList("file1", "file2")), new HashSet<String>(Arrays.asList(listAll)));
    for (String f : listAll) {
      assertEquals(4, dir2.fileLength(f));
    }
    dir2.close();
  }

  @Test
  public void test4() throws IOException {
    Path path = new Path(_root, "dir4");
    {
      HdfsDirectory dir1 = new HdfsDirectory(_configuration, path);
      createFile(dir1, "file1");
      createFile(dir1, "file2");
      createFile(dir1, "file3");
      IndexOutput output = dir1.createOutput("file2", IOContext.DEFAULT);
      output.writeLong(1L);
      output.close();
      dir1.close();
    }

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, path);
    String[] listAll = dir2.listAll();
    assertEquals(3, listAll.length);
    assertEquals(new HashSet<String>(Arrays.asList("file1", "file2", "file3")),
        new HashSet<String>(Arrays.asList(listAll)));
    for (String f : listAll) {
      if (f.equals("file2")) {
        assertEquals(8, dir2.fileLength(f));
      } else {
        assertEquals(4, dir2.fileLength(f));
      }
    }
    dir2.close();
  }

  private void createFile(HdfsDirectory dir, String name) throws IOException {
    IndexOutput output = dir.createOutput(name, IOContext.DEFAULT);
    output.writeInt(1);
    output.close();
  }

  private void createFiles(Configuration configuration, int numberOfDirs, int numberOfFiles, Path path,
      HdfsDirectory mainDir) throws IOException {
    FileSystem fileSystem = path.getFileSystem(configuration);
    for (int d = 0; d < numberOfDirs; d++) {
      Path dir = new Path(path, "dir." + d);
      fileSystem.mkdirs(dir);
      for (int f = 0; f < numberOfFiles; f++) {
        Path p = new Path(dir, "file." + f);
        FSDataOutputStream outputStream = fileSystem.create(p);
        outputStream.write(1);
        outputStream.close();
      }
      HdfsDirectory subDir = new HdfsDirectory(configuration, dir);
      for (String file : subDir.listAll()) {
        subDir.copy(mainDir, file, UUID.randomUUID().toString(), IOContext.READ);
      }
      subDir.close();
    }
  }
}
