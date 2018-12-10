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
package org.apache.blur.manager.writer;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.MiniCluster;
import org.apache.blur.store.BlockCacheDirectoryFactoryV2;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class SharedMergeSchedulerThroughputTestIT {

  @Test
  public void voidTest() {

  }

  public void test() throws IOException {
    MiniCluster miniCluster = new MiniCluster();
    miniCluster.startDfs("./tmp/hdfs");
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new StandardAnalyzer(Version.LUCENE_43));
    SharedMergeScheduler sharedMergeScheduler = new SharedMergeScheduler(10);
    conf.setMergeScheduler(sharedMergeScheduler.getMergeScheduler());
    Configuration configuration = new Configuration();
    URI fileSystemUri = miniCluster.getFileSystemUri();
    Path path = new Path(fileSystemUri.toString() + "/merge-test");
    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.delete(path, true);
    HdfsDirectory directory = new HdfsDirectory(configuration, path);
    BlurConfiguration blurConfiguration = new BlurConfiguration();
    BlockCacheDirectoryFactoryV2 factory = new BlockCacheDirectoryFactoryV2(blurConfiguration, 1000000l);

    Directory cdir = factory.newDirectory("t", "s", directory, null);
    IndexWriter writer = new IndexWriter(cdir, conf);
    Random random = new Random(1);
    StringBuilder stringBuilder = new StringBuilder();
    long s = System.nanoTime();
    for (int i = 0; i < 250000; i++) {
      if (i % 5000 == 0) {
        System.out.println(i);
      }
      stringBuilder.setLength(0);
      for (int w = 0; w < 2000; w++) {
        stringBuilder.append(Integer.toString(random.nextInt(100000))).append(' ');
      }
      Document document = new Document();
      document.add(new TextField("body", stringBuilder.toString(), Store.YES));
      writer.addDocument(document);
    }
    writer.close(true);
    sharedMergeScheduler.close();
    factory.close();
    long e = System.nanoTime();
    System.out.println("Total Time [" + (e - s) / 1000000.0 + " ms]");
    miniCluster.shutdownDfs();
  }

}
