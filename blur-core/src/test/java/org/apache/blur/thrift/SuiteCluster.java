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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.MiniCluster;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.GCWatcher;
import org.apache.blur.utils.JavaHome;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Splitter;

public class SuiteCluster {

  private static final String SEPARATOR = ":";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String TEST_CLASSES = "test-classes";

  public static final String YARN_SITE_XML = "yarn-site.xml";
  public static final String MAPRED_SITE_XML = "mapred-site.xml";

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_SuiteCluster"));
  private static MiniCluster cluster;
  public static boolean externalProcesses = false;
  private static Class<?> _setupClass = null;

  static {
    GCWatcher.init(0.60);
  }

  public static void shutdownMiniCluster(Class<?> setupClass) throws IOException {
    if (cluster != null && _setupClass.equals(setupClass)) {
      removeSiteFiles();
      cluster.shutdownMrMiniCluster();
      cluster.shutdownBlurCluster();
      File file = new File(TMPDIR, "blur-cluster-test");
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
      }
      cluster = null;
    }
  }

  public synchronized static void setupMiniCluster(Class<?> setupClass) throws IOException {
    if (SuiteCluster.isClusterSetup()) {
      return;
    }
    JavaHome.checkJavaHome();
    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
    testDirectory.mkdirs();
    testDirectory.delete();
    if (cluster == null) {
      _setupClass = setupClass;
      removeSiteFiles();
      cluster = new MiniCluster();
      cluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 2, true, externalProcesses);
      cluster.startMrMiniCluster();
      writeSiteFiles(cluster.getMRConfiguration());
      System.out.println("MiniCluster started at " + cluster.getControllerConnectionStr());
    }
  }

  private static void writeSiteFiles(Configuration configuration) throws IOException {
    String name = MAPRED_SITE_XML;
    if (cluster.useYarn()) {
      name = YARN_SITE_XML;
    }
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    for (String path : Splitter.on(SEPARATOR).split(classPath)) {
      File file = new File(path);
      if (file.getName().equals(TEST_CLASSES)) {
        writeFile(new File(file, name), configuration);
        return;
      }
    }
  }

  private static void writeFile(File file, Configuration configuration) throws FileNotFoundException, IOException {
    FileOutputStream outputStream = new FileOutputStream(file);
    configuration.writeXml(outputStream);
    outputStream.close();
  }

  private static void removeSiteFiles() throws IOException {
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    for (String path : Splitter.on(SEPARATOR).split(classPath)) {
      File file = new File(path);
      if (file.getName().equals(TEST_CLASSES)) {
        File f1 = new File(file, MAPRED_SITE_XML);
        File f2 = new File(file, YARN_SITE_XML);
        f1.delete();
        f2.delete();
        return;
      }
    }
  }

  public static Iface getClient() throws IOException {
    String zkConnectionString = cluster.getZkConnectionString();
    BlurConfiguration blurConfiguration = new BlurConfiguration();
    blurConfiguration.set(BlurConstants.BLUR_ZOOKEEPER_CONNECTION, zkConnectionString);
    return BlurClient.getClient(blurConfiguration);
  }

  public Configuration getMRConfiguration() {
    return cluster.getMRConfiguration();
  }

  public Configuration getHdfsConfiguration() {
    return cluster.getConfiguration();
  }

  public static boolean isClusterSetup() {
    return cluster != null;
  }

  public static FileSystem getFileSystem() throws IOException {
    return cluster.getFileSystem();
  }

  public static String getFileSystemUri() throws IOException {
    return cluster.getFileSystemUri().toString();
  }

  public static String getZooKeeperConnStr() {
    return cluster.getZkConnectionString();
  }

  public static String getControllerConnectionStr() {
    return cluster.getControllerConnectionStr();
  }

  public static MiniCluster getMiniCluster() {
    return cluster;
  }

  public static String getZooKeeperConnStr(Class<?> clazz) throws IOException {
    String name = clazz.getName();
    String zooKeeperConnStr = getZooKeeperConnStr();
    ZooKeeperClient zk = new ZooKeeperClient(zooKeeperConnStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    String path = "/" + name.replace(".", "_");
    ZkUtils.mkNodesStr(zk, path);
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    String zkConn = zooKeeperConnStr + path;
    return zkConn;
  }

}
