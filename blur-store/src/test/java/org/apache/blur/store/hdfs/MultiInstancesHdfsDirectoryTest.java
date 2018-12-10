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

import java.io.File;
import java.io.IOException;

import org.apache.blur.HdfsMiniClusterUtil;
import org.apache.blur.utils.JavaHome;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiInstancesHdfsDirectoryTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir",
      "./target/tmp_MultiInstancesHdfsDirectoryTest"));

  private static Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;
  private static Path _root;

  @BeforeClass
  public static void setupClass() throws IOException {
    JavaHome.checkJavaHome();
    _cluster = HdfsMiniClusterUtil.startDfs(_configuration, true, TMPDIR.getAbsolutePath());
    _root = new Path(_cluster.getFileSystem().getUri() + "/");
  }

  @AfterClass
  public static void tearDown() {
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Test
  public void testMultiInstancesHdfsDirectoryTest1() throws IOException, InterruptedException {
    HdfsDirectory dir1 = new HdfsDirectory(_configuration, new Path(_root, "dir"));
    

    IndexOutput output = dir1.createOutput("a", IOContext.DEFAULT);
    output.writeInt(1234);
    output.close();
    
    HdfsDirectory dir2 = new HdfsDirectory(_configuration, new Path(_root, "dir"));

    IndexInput input = dir2.openInput("a", IOContext.READ);
    assertEquals(4, input.length());
    assertEquals(1234, input.readInt());
    input.close();

    dir1.close();
    dir2.close();
  }

}
