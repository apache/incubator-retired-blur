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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TreeSet;

import org.apache.blur.HdfsMiniClusterUtil;
import org.apache.blur.kvs.HdfsKeyValueStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FastHdfsKeyValueDirectoryTest {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/tmp_HdfsKeyValueStoreTest"));

  private Configuration _configuration = new Configuration();
  private static MiniDFSCluster _cluster;

  private static Timer _timer;
  private Path _path;

  @BeforeClass
  public static void startCluster() {
    Configuration conf = new Configuration();
    _cluster = HdfsMiniClusterUtil.startDfs(conf, true, TMPDIR.getAbsolutePath());
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
  public void testMultipleWritersOpenOnSameDirectory() throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    FastHdfsKeyValueDirectory directory = new FastHdfsKeyValueDirectory(false, _timer, _configuration, new Path(_path,
        "test_multiple"));
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

  @Test
  public void testMulipleCommitsAndReopens() throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    conf.setMergeScheduler(new SerialMergeScheduler());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);

    Set<String> fileSet = new TreeSet<String>();
    long seed = new Random().nextLong();
    System.out.println("Seed:" + seed);
    Random random = new Random(seed);
    int docCount = 0;
    int passes = 10;
    byte[] segmentsGenContents = null;
    for (int run = 0; run < passes; run++) {
      final FastHdfsKeyValueDirectory directory = new FastHdfsKeyValueDirectory(false, _timer, _configuration,
          new Path(_path, "test_multiple_commits_reopens"));
      if (segmentsGenContents != null) {
        byte[] segmentsGenContentsCurrent = readSegmentsGen(directory);
        assertTrue(Arrays.equals(segmentsGenContents, segmentsGenContentsCurrent));
      }
      assertFiles(fileSet, run, -1, directory);
      assertEquals(docCount, getDocumentCount(directory));
      IndexWriter writer = new IndexWriter(directory, conf.clone());
      int numberOfCommits = random.nextInt(100);
      for (int i = 0; i < numberOfCommits; i++) {
        assertFiles(fileSet, run, i, directory);
        addDocuments(writer, random.nextInt(100));
        // Before Commit
        writer.commit();
        // After Commit

        // Set files after commit
        {
          fileSet.clear();
          List<IndexCommit> listCommits = DirectoryReader.listCommits(directory);
          assertEquals(1, listCommits.size());
          IndexCommit indexCommit = listCommits.get(0);
          fileSet.addAll(indexCommit.getFileNames());
        }
        segmentsGenContents = readSegmentsGen(directory);
      }
      docCount = getDocumentCount(directory);
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void testOpenDirectoryAndReopenEmptyDirectory() throws IOException, InterruptedException {
    FastHdfsKeyValueDirectory directory1 = new FastHdfsKeyValueDirectory(false, _timer, _configuration, new Path(_path,
        "testOpenDirectoryAndReopenEmptyDirectory"), HdfsKeyValueStore.DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE, 5000L);
    assertTrue(Arrays.equals(new String[] {}, directory1.listAll()));
    FastHdfsKeyValueDirectory directory2 = new FastHdfsKeyValueDirectory(false, _timer, _configuration, new Path(_path,
        "testOpenDirectoryAndReopenEmptyDirectory"));
    assertTrue(Arrays.equals(new String[] {}, directory2.listAll()));
  }

  private byte[] readSegmentsGen(FastHdfsKeyValueDirectory directory) throws IOException {
    boolean fileExists = directory.fileExists("segments.gen");
    if (!fileExists) {
      return null;
    }
    IndexInput input = directory.openInput("segments.gen", IOContext.READ);
    byte[] data = new byte[(int) input.length()];
    input.readBytes(data, 0, data.length);
    return data;
  }

  private int getDocumentCount(Directory directory) throws IOException {
    if (DirectoryReader.indexExists(directory)) {
      DirectoryReader reader = DirectoryReader.open(directory);
      int maxDoc = reader.maxDoc();
      reader.close();
      return maxDoc;
    }
    return 0;
  }

  private void assertFiles(Set<String> expected, int run, int commit, FastHdfsKeyValueDirectory directory)
      throws IOException {
    Set<String> actual;
    if (DirectoryReader.indexExists(directory)) {
      List<IndexCommit> listCommits = DirectoryReader.listCommits(directory);
      // assertEquals(1, listCommits.size());
      IndexCommit indexCommit = listCommits.get(0);
      actual = new TreeSet<String>(indexCommit.getFileNames());
    } else {
      actual = new TreeSet<String>();
    }

    Set<String> missing = new TreeSet<String>(expected);
    missing.removeAll(actual);
    Set<String> extra = new TreeSet<String>(actual);
    extra.removeAll(expected);
    assertEquals("Pass [" + run + "] Missing Files " + " Extra Files " + extra + "", expected, actual);
  }

  private void addDocuments(IndexWriter writer, int numberOfDocs) throws IOException {
    for (int i = 0; i < numberOfDocs; i++) {
      addDoc(writer, getDoc(i));
    }
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
