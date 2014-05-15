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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class HdfsDirectorySymlinkTest {

  private Path _base;
  private Configuration _configuration;
  private FileSystem _fileSystem;

  @Before
  public void setup() throws IOException {
    _base = new Path("./target/tmp/HdfsDirectoryTest");
    _configuration = new Configuration();

    _fileSystem = _base.getFileSystem(_configuration);
    _fileSystem.delete(_base, true);
    _fileSystem.mkdirs(_base);
  }

  @Test
  public void testSymlink() throws IOException {
    HdfsDirectory dir1 = new HdfsDirectory(_configuration, new Path(_base, "dir1"));
    IndexOutput output = dir1.createOutput("file1", IOContext.DEFAULT);
    output.writeLong(12345);
    output.close();

    assertTrue(dir1.fileExists("file1"));

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, new Path(_base, "dir2"));
    dir1.copy(dir2, "file1", "file2", IOContext.DEFAULT);

    assertTrue(dir2.fileExists("file2"));
    assertEquals(8, dir2.fileLength("file2"));

    String[] listAll = dir2.listAll();
    assertEquals(1, listAll.length);
    assertEquals("file2", listAll[0]);

    IndexInput input = dir2.openInput("file2", IOContext.DEFAULT);
    assertEquals(12345, input.readLong());
    input.close();

    dir2.deleteFile("file2");

    assertFalse(dir2.fileExists("file2"));
    assertTrue(dir1.fileExists("file1"));

    dir2.close();
    dir1.close();
  }

  @Test
  public void testSymlinkWithIndexes() throws IOException {
    HdfsDirectory dir1 = new HdfsDirectory(_configuration, new Path(_base, "dir1"));
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    IndexWriter writer1 = new IndexWriter(dir1, conf.clone());
    writer1.addDocument(getDoc());
    writer1.close();

    HdfsDirectory dir2 = new HdfsDirectory(_configuration, new Path(_base, "dir2"));
    IndexWriter writer2 = new IndexWriter(dir2, conf.clone());
    writer2.addIndexes(dir1);
    writer2.close();

    DirectoryReader reader1 = DirectoryReader.open(dir1);
    DirectoryReader reader2 = DirectoryReader.open(dir2);

    assertEquals(1, reader1.maxDoc());
    assertEquals(1, reader2.maxDoc());
    assertEquals(1, reader1.numDocs());
    assertEquals(1, reader2.numDocs());

    Document document1 = reader1.document(0);
    Document document2 = reader2.document(0);

    assertEquals(document1.get("id"), document2.get("id"));
  }

  private Document getDoc() {
    Document document = new Document();
    document.add(new StringField("id", UUID.randomUUID().toString(), Store.YES));
    return document;
  }

}
