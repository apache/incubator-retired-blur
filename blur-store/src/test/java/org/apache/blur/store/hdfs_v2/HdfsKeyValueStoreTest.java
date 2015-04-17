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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Timer;

import org.apache.blur.HdfsMiniClusterUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HdfsKeyValueStoreTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_HdfsKeyValueStoreTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;

  private static Timer _timer;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
    _timer = new Timer("IndexImporter", true);
  }

  @AfterClass
  public static void stopCluster() {
    _timer.cancel();
    _timer.purge();
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    _path = new Path("/test").makeQualified(fileSystem);
    fileSystem.delete(_path, true);
  }

  @Test
  public void testPutGet() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_timer, _configuration, _path);
    store.put(toBytesRef("a"), toBytesRef("value1"));
    store.put(toBytesRef("b"), toBytesRef("value2"));
    store.sync();
    BytesRef value = new BytesRef();
    store.get(toBytesRef("a"), value);
    assertEquals(new BytesRef("value1"), value);
    store.get(toBytesRef("b"), value);
    assertEquals(new BytesRef("value2"), value);
    store.close();
  }

  @Test
  public void testPutGetDelete() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_timer, _configuration, _path);
    store.put(toBytesRef("a"), toBytesRef("value1"));
    store.put(toBytesRef("b"), toBytesRef("value2"));
    store.sync();
    BytesRef value = new BytesRef();
    store.get(toBytesRef("a"), value);
    assertEquals(new BytesRef("value1"), value);
    store.get(toBytesRef("b"), value);
    assertEquals(new BytesRef("value2"), value);

    store.delete(toBytesRef("b"));
    store.sync();
    assertFalse(store.get(toBytesRef("b"), value));
    store.close();
  }

  @Test
  public void testPutGetReopen() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_timer, _configuration, _path);
    store1.put(toBytesRef("a"), toBytesRef("value1"));
    store1.put(toBytesRef("b"), toBytesRef("value2"));
    store1.sync();
    BytesRef value1 = new BytesRef();
    store1.get(toBytesRef("a"), value1);
    assertEquals(new BytesRef("value1"), value1);
    store1.get(toBytesRef("b"), value1);
    assertEquals(new BytesRef("value2"), value1);
    store1.close();

    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_timer, _configuration, _path);
    BytesRef value2 = new BytesRef();
    store2.get(toBytesRef("a"), value2);
    assertEquals(new BytesRef("value1"), value2);
    store2.get(toBytesRef("b"), value2);
    assertEquals(new BytesRef("value2"), value2);
    store2.close();
  }

  @Test
  public void testFileRolling() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_timer, _configuration, _path, 1000);
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    store.put(new BytesRef("a"), new BytesRef(""));
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

  @Test
  public void testFileGC() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_timer, _configuration, _path, 1000);
    store.put(new BytesRef("a"), new BytesRef(""));
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    store.cleanupOldFiles();
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

  // @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTime() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_timer, _configuration, _path);
    listFiles();
    store1.put(new BytesRef("a1"), new BytesRef(new byte[2000]));
    listFiles();
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_timer, _configuration, _path);
    listFiles();
    store2.put(new BytesRef("a1"), new BytesRef(new byte[1000]));
    listFiles();
    store1.put(new BytesRef("a2"), new BytesRef(new byte[2000]));
    listFiles();
    store2.put(new BytesRef("a2"), new BytesRef(new byte[1000]));
    listFiles();
    store1.put(new BytesRef("a3"), new BytesRef(new byte[2000]));
    listFiles();
    store2.put(new BytesRef("a3"), new BytesRef(new byte[1000]));
    listFiles();
    try {
      store1.sync();
      fail();
    } catch (Exception e) {

    }
    store2.sync();
    store1.close();
    store2.close();

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_timer, _configuration, _path);
    Iterable<Entry<BytesRef, BytesRef>> scan = store3.scan(null);
    for (Entry<BytesRef, BytesRef> e : scan) {
      System.out.println(e.getValue().length);
    }
    store3.close();
  }

  @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTimeSmallFiles() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_timer, _configuration, _path, 1000);
    store1.put(new BytesRef("a1"), new BytesRef(new byte[2000]));
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_timer, _configuration, _path, 1000);
    store2.put(new BytesRef("a1"), new BytesRef(new byte[1000]));
    try {
      store1.put(new BytesRef("a2"), new BytesRef(new byte[2000]));
      fail();
    } catch (Exception e) {
      // Should throw exception
      store1.close();
    }
    store2.put(new BytesRef("a2"), new BytesRef(new byte[1000]));
    store2.put(new BytesRef("a3"), new BytesRef(new byte[1000]));
    store2.sync();
    store2.close();

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_timer, _configuration, _path);
    Iterable<Entry<BytesRef, BytesRef>> scan = store3.scan(null);
    for (Entry<BytesRef, BytesRef> e : scan) {
      System.out.println(e.getValue().length);
    }
    store3.close();
  }

  private void listFiles() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    for (FileStatus status : fileSystem.listStatus(_path)) {
      System.out.println(status.getPath() + " " + status.getLen());
    }
  }

  private BytesRef toBytesRef(String s) {
    return new BytesRef(s);
  }

}
