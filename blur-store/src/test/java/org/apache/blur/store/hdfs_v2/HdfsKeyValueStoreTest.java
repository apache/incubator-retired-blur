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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

public class HdfsKeyValueStoreTest {

  private Path _path = new Path("hdfs://127.0.0.1:9000/test");
  private Configuration _configuration = new Configuration();

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
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

  private BytesRef toBytesRef(String s) {
    return new BytesRef(s);
  }

}
