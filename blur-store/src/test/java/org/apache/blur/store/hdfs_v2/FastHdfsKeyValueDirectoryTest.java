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

import java.io.File;
import java.io.IOException;

import org.apache.blur.HdfsMiniClusterUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FastHdfsKeyValueDirectoryTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_HdfsKeyValueStoreTest"));

  private Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    Configuration conf = new Configuration();
    _cluster = HdfsMiniClusterUtil.startDfs(conf, true, TMPDIR.getAbsolutePath());
  }

  @AfterClass
  public static void stopCluster() {
    HdfsMiniClusterUtil.shutdownDfs(_cluster);
  }

  @Before
  public void setup() throws IOException {
    FileSystem fileSystem = _cluster.getFileSystem();
    _path = new Path("/test").makeQualified(fileSystem);
    fileSystem.delete(_path, true);
  }

  @Test
  public void testMultipleWritersOpenOnSameDirectory() throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    FastHdfsKeyValueDirectory directory = new FastHdfsKeyValueDirectory(_configuration,
        new Path(_path, "test_multiple"));
    IndexWriter writer1 = new IndexWriter(directory, config.clone());
    addDoc(writer1, getDoc(1));
    IndexWriter writer2 = new IndexWriter(directory, config.clone());
    addDoc(writer2, getDoc(2));
    writer1.close();
    writer2.close();

    DirectoryReader reader = DirectoryReader.open(directory);
    int maxDoc = reader.maxDoc();
    assertEquals(1, maxDoc);
    Document document = reader.document(0);
    assertEquals("2", document.get("id"));
    reader.close();
  }

  private void addDoc(IndexWriter writer, Document doc) throws IOException {
    writer.addDocument(doc);
  }

  private Document getDoc(int i) {
    Document document = new Document();
    document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
    return document;
  }
}
