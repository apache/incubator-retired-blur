package org.apache.blur.thrift;

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
import java.io.File;
import java.io.IOException;

import org.apache.blur.thrift.generated.Blur.Iface;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseClusterTest {
  protected static String TABLE_PATH = new File("./target/tmp/test-data/test-tables").getAbsolutePath();
  private static boolean _managing;

  @BeforeClass
  public static void startup() throws IOException {
    if (!SuiteCluster.isClusterSetup()) {
      SuiteCluster.setupMiniCluster();
      _managing = true;
    }
    File file = new File("test-data");
    if (file.exists()) {
      rmr(file);
    }
  }

  private static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    if (_managing) {
      SuiteCluster.shutdownMiniCluster();
    }
  }

  public Iface getClient() throws IOException {
    return SuiteCluster.getClient();
  }
  
  protected String getZkConnString() {
    return SuiteCluster.getZooKeeperConnStr();
  }
}
