package org.apache.blur.mapreduce.lib.v2;

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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.blur.server.TableContext;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DirectIndexingDriverMiniClusterTest {

  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  private static MiniMRCluster mr;
  private static final Path TEST_ROOT_DIR = new Path("./target/tmp/DirectIndexingDriverTest_tmp");
  private static JobConf jobConf;
  private static final Path outDir = new Path(TEST_ROOT_DIR + "/out");
  private static final Path inDir = new Path(TEST_ROOT_DIR + "/in");

  @BeforeClass
  public static void setupTest() throws Exception {
    System.setProperty("test.build.data", "./target/DirectIndexingDriverTest/data");
    System.setProperty("hadoop.log.dir", "./target/DirectIndexingDriverTest/hadoop_log");
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
    mr = new MiniMRCluster(1, "file:///", 1);
    jobConf = mr.createJobConf();
    BufferStore.initNewBuffer(128, 128 * 128);
  }

  @AfterClass
  public static void teardown() {
    if (mr != null) {
      mr.shutdown();
    }
    rm(new File("build"));
  }

  private static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  @Before
  public void setup() throws IllegalArgumentException, IOException {
    TableContext.clear();
    if (localFs.exists(inDir)) {
      assertTrue(localFs.delete(inDir, true));
    }
    if (localFs.exists(outDir)) {
      assertTrue(localFs.delete(outDir, true));
    }
  }

  @Test
  public void testBlurOutputFormat() throws Exception {
//    DirectIndexingDriverTest.createInputDocument(localFs, jobConf, inDir);
//    DirectIndexingDriver driver = new DirectIndexingDriver();
//    driver.setConf(jobConf);
//    driver.run(new String[] { inDir.toString(), outDir.toString() });
  }

}
