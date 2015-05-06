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

import org.apache.blur.BlurConfiguration;
import org.apache.blur.MiniCluster;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SuiteCluster {
  private static final Log log = LogFactory.getLog(SuiteCluster.class);
  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_SuiteCluster"));
  private static MiniCluster cluster;

  public static void shutdownMiniCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdownBlurCluster();
      File file = new File(TMPDIR, "blur-cluster-test");
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
      }
      cluster = null;
    }
  }

  public static void setupMiniCluster() throws IOException {
    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
    testDirectory.mkdirs();

    testDirectory.delete();
    if (cluster == null) {
      cluster = new MiniCluster();
      cluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true, false);

      System.out.println("MiniCluster started at " + cluster.getControllerConnectionStr());

    }
  }

  public static Iface getClient() throws IOException {
    String zkConnectionString = cluster.getZkConnectionString();
    BlurConfiguration blurConfiguration = new BlurConfiguration();
    blurConfiguration.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, zkConnectionString);
    return BlurClient.getClient(blurConfiguration);
  }

  public static boolean isClusterSetup() {
    return cluster != null;
  }

  public static String getFileSystemUri() {
    try {
      return cluster.getFileSystemUri().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getZooKeeperConnStr() {
    return cluster.getZkConnectionString();
  }

}
