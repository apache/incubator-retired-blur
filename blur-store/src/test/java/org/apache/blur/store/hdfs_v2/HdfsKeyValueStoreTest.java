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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HdfsKeyValueStoreTest {

  private static final Log LOG = LogFactory.getLog(HdfsKeyValueStoreTest.class);
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_HdfsKeyValueStoreTest"));

  private Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    Configuration conf = new Configuration();
    startDfs(conf, true, TMPDIR.getAbsolutePath());
  }

  @AfterClass
  public static void stopCluster() {
    shutdownDfs();
  }

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    _path = new Path("/test").makeQualified(fileSystem);
    fileSystem.delete(_path, true);
  }

  @Test
  public void testPutGet() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_configuration, _path);
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
    HdfsKeyValueStore store = new HdfsKeyValueStore(_configuration, _path);
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
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_configuration, _path);
    store1.put(toBytesRef("a"), toBytesRef("value1"));
    store1.put(toBytesRef("b"), toBytesRef("value2"));
    store1.sync();
    BytesRef value1 = new BytesRef();
    store1.get(toBytesRef("a"), value1);
    assertEquals(new BytesRef("value1"), value1);
    store1.get(toBytesRef("b"), value1);
    assertEquals(new BytesRef("value2"), value1);
    store1.close();

    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_configuration, _path);
    BytesRef value2 = new BytesRef();
    store2.get(toBytesRef("a"), value2);
    assertEquals(new BytesRef("value1"), value2);
    store2.get(toBytesRef("b"), value2);
    assertEquals(new BytesRef("value2"), value2);
    store2.close();
  }

  @Test
  public void testFileRolling() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_configuration, _path, 1000);
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

  @Test
  public void testFileGC() throws IOException {
    HdfsKeyValueStore store = new HdfsKeyValueStore(_configuration, _path, 1000);
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    assertEquals(1, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.put(new BytesRef("a"), new BytesRef(new byte[2000]));
    assertEquals(2, fileSystem.listStatus(_path).length);
    store.close();
  }

//  @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTime() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_configuration, _path);
    listFiles();
    store1.put(new BytesRef("a1"), new BytesRef(new byte[2000]));
    listFiles();
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_configuration, _path);
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

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_configuration, _path);
    Iterable<Entry<BytesRef, BytesRef>> scan = store3.scan(null);
    for (Entry<BytesRef, BytesRef> e : scan) {
      System.out.println(e.getValue().length);
    }
    store3.close();
  }

  @Test
  public void testTwoKeyStoreInstancesWritingAtTheSameTimeSmallFiles() throws IOException {
    HdfsKeyValueStore store1 = new HdfsKeyValueStore(_configuration, _path, 1000);
    store1.put(new BytesRef("a1"), new BytesRef(new byte[2000]));
    HdfsKeyValueStore store2 = new HdfsKeyValueStore(_configuration, _path, 1000);
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

    HdfsKeyValueStore store3 = new HdfsKeyValueStore(_configuration, _path);
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

  public static void startDfs(Configuration conf, boolean format, String path) {
    String perm;
    Path p = new Path(new File("./target").getAbsolutePath());
    try {
      FileSystem fileSystem = p.getFileSystem(conf);
      FileStatus fileStatus = fileSystem.getFileStatus(p);
      FsPermission permission = fileStatus.getPermission();
      perm = permission.getUserAction().ordinal() + "" + permission.getGroupAction().ordinal() + ""
          + permission.getOtherAction().ordinal();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("dfs.datanode.data.dir.perm=" + perm);
    conf.set("dfs.datanode.data.dir.perm", perm);
    System.setProperty("test.build.data", path);
    try {
      _cluster = new MiniDFSCluster(conf, 1, true, (String[]) null);
      _cluster.waitActive();
    } catch (Exception e) {
      LOG.error("error opening file system", e);
      throw new RuntimeException(e);
    }
  }

  public static void shutdownDfs() {
    if (_cluster != null) {
      LOG.info("Shutting down Mini DFS ");
      try {
        _cluster.shutdown();
      } catch (Exception e) {
        // / Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
      try {
        FileSystem fs = _cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
        FileSystem.closeAll();
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }

      // This has got to be one of the worst hacks I have ever had to do.
      // This is needed to shutdown 2 thread pools that are not shutdown by
      // themselves.
      ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
      Thread[] threads = new Thread[100];
      int enumerate = threadGroup.enumerate(threads);
      for (int i = 0; i < enumerate; i++) {
        Thread thread = threads[i];
        if (thread.getName().startsWith("pool")) {
          if (thread.isAlive()) {
            thread.interrupt();
            LOG.info("Stopping ThreadPoolExecutor [" + thread.getName() + "]");
            Object target = getField(Thread.class, thread, "target");
            if (target != null) {
              ThreadPoolExecutor e = (ThreadPoolExecutor) getField(ThreadPoolExecutor.class, target, "this$0");
              if (e != null) {
                e.shutdownNow();
              }
            }
            try {
              LOG.info("Waiting for thread pool to exit [" + thread.getName() + "]");
              thread.join();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
  }

  private static Object getField(Class<?> c, Object o, String fieldName) {
    try {
      Field field = c.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(o);
    } catch (NoSuchFieldException e) {
      try {
        Field field = o.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(o);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
