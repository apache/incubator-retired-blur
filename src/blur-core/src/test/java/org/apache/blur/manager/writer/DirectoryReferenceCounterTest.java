package org.apache.blur.manager.writer;

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

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.lucene.store.refcounter.DirectoryReferenceCounter;
import org.apache.blur.lucene.store.refcounter.DirectoryReferenceFileGC;
import org.apache.blur.lucene.store.refcounter.IndexInputCloser;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

public class DirectoryReferenceCounterTest {

  @Test
  public void testDirectoryReferenceCounterTestError() throws CorruptIndexException, IOException {
    Directory directory = wrap(new RAMDirectory());
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    int size = 100;
    IndexReader[] readers = new IndexReader[size];
    for (int i = 0; i < size; i++) {
      writer.addDocument(getDoc());
      readers[i] = DirectoryReader.open(writer, true);
      writer.forceMerge(1);
    }

    try {
      for (int i = 0; i < size; i++) {
        checkReader(readers[i], i);
      }
      fail();
    } catch (Exception e) {
      // should error
    }
  }

  @Test
  public void testDirectoryReferenceCounter() throws CorruptIndexException, LockObtainFailedException, IOException, InterruptedException {
    Directory directory = wrap(new RAMDirectory());
    DirectoryReferenceFileGC gc = new DirectoryReferenceFileGC();
    gc.init();
    IndexInputCloser closer = new IndexInputCloser();
    closer.init();
    DirectoryReferenceCounter counter = new DirectoryReferenceCounter(directory, gc, closer);
    IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(counter, conf);
    int size = 100;
    IndexReader[] readers = new IndexReader[size];
    for (int i = 0; i < size; i++) {
      writer.addDocument(getDoc());
      writer.forceMerge(1);
      readers[i] = DirectoryReader.open(writer, true);
    }

    for (int i = 0; i < size; i++) {
      assertEquals(i + 1, readers[i].numDocs());
      checkReader(readers[i], i);
    }

    String[] listAll = directory.listAll();

    for (int i = 0; i < size - 1; i++) {
      readers[i].close();
    }

    for (int i = 0; i < 1000; i++) {
      gc.run();
      Thread.sleep(1);
    }

    IndexReader last = readers[size - 1];

    assertEquals(100, last.numDocs());

    assertTrue(listAll.length > directory.listAll().length);

    last.close();
    writer.close();
    gc.close();
  }

  private Document getDoc() {
    Document document = new Document();
    FieldType type = new FieldType();
    type.setIndexed(true);
    type.setOmitNorms(true);
    type.setTokenized(false);
    type.setStored(true);
    document.add(new Field("id", "value", type));
    return document;
  }

  private void checkReader(IndexReader indexReader, int size) throws CorruptIndexException, IOException {
    for (int i = 0; i < size; i++) {
      Document document = indexReader.document(i);
      String value = document.get("id");
      assertEquals("value", value);
    }
  }

  // This class is use simulate what would happen with a directory that will
  // forcefully delete files even if they are still in use. e.g. HDFSDirectory
  public static Directory wrap(final RAMDirectory ramDirectory) {
    return new Directory() {
      private Directory d = ramDirectory;
      private Collection<String> deletedFiles = new LinkedBlockingQueue<String>();

      @Override
      public void deleteFile(String name) throws IOException {
        deletedFiles.add(name);
        d.deleteFile(name);
      }

      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return d.createOutput(name, context);
      }

      @Override
      public void sync(Collection<String> names) throws IOException {
        d.sync(names);
      }

      @Override
      public IndexInput openInput(String name, IOContext context) throws IOException {
        return wrap(d.openInput(name, context), deletedFiles, name);
      }

      @Override
      public void clearLock(String name) throws IOException {
        d.clearLock(name);
      }

      @Override
      public void close() throws IOException {
        d.close();
      }

      @Override
      public void setLockFactory(LockFactory lockFactory) throws IOException {
        d.setLockFactory(lockFactory);
      }

      @Override
      public String getLockID() {
        return d.getLockID();
      }

      @Override
      public boolean equals(Object arg0) {
        return d.equals(arg0);
      }

      @Override
      public boolean fileExists(String name) throws IOException {
        return d.fileExists(name);
      }

      @Override
      public long fileLength(String name) throws IOException {
        return d.fileLength(name);
      }

      @Override
      public LockFactory getLockFactory() {
        return d.getLockFactory();
      }

      @Override
      public int hashCode() {
        return d.hashCode();
      }

      @Override
      public String[] listAll() throws IOException {
        return d.listAll();
      }

      @Override
      public Lock makeLock(String name) {
        return d.makeLock(name);
      }

      @Override
      public String toString() {
        return d.toString();
      }
    };
  }

  public static IndexInput wrap(final IndexInput input, final Collection<String> deletedFiles, final String name) {
    return new IndexInput(input.toString()) {
      private IndexInput in = input;

      private void checkForDeleted() throws IOException {
        if (deletedFiles.contains(name)) {
          throw new IOException("File [" + name + "] does not exist");
        }
      }

      @Override
      public void close() throws IOException {
        checkForDeleted();
        in.close();
      }

      @Override
      public short readShort() throws IOException {
        checkForDeleted();
        return in.readShort();
      }

      @Override
      public void seek(long pos) throws IOException {
        checkForDeleted();
        in.seek(pos);
      }

      @Override
      public int readInt() throws IOException {
        checkForDeleted();
        return in.readInt();
      }

      @Override
      public int readVInt() throws IOException {
        checkForDeleted();
        return in.readVInt();
      }

      @Override
      public String toString() {
        return in.toString();
      }

      @Override
      public long readLong() throws IOException {
        checkForDeleted();
        return in.readLong();
      }

      @Override
      public long readVLong() throws IOException {
        checkForDeleted();
        return in.readVLong();
      }

      @Override
      public String readString() throws IOException {
        checkForDeleted();
        return in.readString();
      }

      @Override
      public IndexInput clone() {
        return super.clone();
      }

      @Override
      public boolean equals(Object obj) {
        return in.equals(obj);
      }

      @Override
      public long getFilePointer() {
        return in.getFilePointer();
      }

      @Override
      public int hashCode() {
        return in.hashCode();
      }

      @Override
      public byte readByte() throws IOException {
        checkForDeleted();
        return in.readByte();
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) throws IOException {
        checkForDeleted();
        in.readBytes(b, offset, len);
      }

      @Override
      public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        checkForDeleted();
        in.readBytes(b, offset, len, useBuffer);
      }

      @Override
      public long length() {
        return in.length();
      }

      @Override
      public Map<String, String> readStringStringMap() throws IOException {
        checkForDeleted();
        return in.readStringStringMap();
      }
    };
  }
}
