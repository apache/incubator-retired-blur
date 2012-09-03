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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.manager.writer.DirectoryReferenceCounter;
import org.apache.blur.manager.writer.DirectoryReferenceFileGC;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;


public class DirectoryReferenceCounterTest {

  @Test
  public void testDirectoryReferenceCounterTestError() throws CorruptIndexException, IOException {
    Directory directory = wrap(new RAMDirectory());
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, conf);
    int size = 100;
    IndexReader[] readers = new IndexReader[size];
    for (int i = 0; i < size; i++) {
      writer.addDocument(getDoc());
      readers[i] = IndexReader.open(writer, true);
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
    DirectoryReferenceCounter counter = new DirectoryReferenceCounter(directory, gc);
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_36, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(counter, conf);
    int size = 100;
    IndexReader[] readers = new IndexReader[size];
    for (int i = 0; i < size; i++) {
      writer.addDocument(getDoc());
      writer.forceMerge(1);
      readers[i] = IndexReader.open(writer, true);
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
    document.add(new Field("id", "value", Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    return document;
  }

  private void checkReader(IndexReader indexReader, int size) throws CorruptIndexException, IOException {
    for (int i = 0; i < size; i++) {
      Document document = indexReader.document(i);
      String value = document.get("id");
      assertEquals(value, "value");
    }
  }

  // This class is use simulate what would happen with a directory that will
  // forcefully delete files even if they are still in use. e.g. HDFSDirectory
  public static Directory wrap(final RAMDirectory ramDirectory) {
    return new Directory() {
      private Directory d = ramDirectory;
      private Collection<String> deletedFiles = new LinkedBlockingQueue<String>();

      @SuppressWarnings("deprecation")
      public void touchFile(String name) throws IOException {
        d.touchFile(name);
      }

      public void deleteFile(String name) throws IOException {
        deletedFiles.add(name);
        d.deleteFile(name);
      }

      public IndexOutput createOutput(String name) throws IOException {
        return d.createOutput(name);
      }

      @SuppressWarnings("deprecation")
      public void sync(String name) throws IOException {
        d.sync(name);
      }

      public void sync(Collection<String> names) throws IOException {
        d.sync(names);
      }

      public IndexInput openInput(String name) throws IOException {
        return wrap(d.openInput(name), deletedFiles, name);
      }

      public IndexInput openInput(String name, int bufferSize) throws IOException {
        return wrap(d.openInput(name, bufferSize), deletedFiles, name);
      }

      public void clearLock(String name) throws IOException {
        d.clearLock(name);
      }

      public void close() throws IOException {
        d.close();
      }

      public void setLockFactory(LockFactory lockFactory) throws IOException {
        d.setLockFactory(lockFactory);
      }

      public String getLockID() {
        return d.getLockID();
      }

      public void copy(Directory to, String src, String dest) throws IOException {
        d.copy(to, src, dest);
      }

      public boolean equals(Object arg0) {
        return d.equals(arg0);
      }

      public boolean fileExists(String name) throws IOException {
        return d.fileExists(name);
      }

      @SuppressWarnings("deprecation")
      public long fileModified(String name) throws IOException {
        return d.fileModified(name);
      }

      public long fileLength(String name) throws IOException {
        return d.fileLength(name);
      }

      public LockFactory getLockFactory() {
        return d.getLockFactory();
      }

      public int hashCode() {
        return d.hashCode();
      }

      public String[] listAll() throws IOException {
        return d.listAll();
      }

      public Lock makeLock(String name) {
        return d.makeLock(name);
      }

      public String toString() {
        return d.toString();
      }
    };
  }

  @SuppressWarnings("deprecation")
  public static IndexInput wrap(final IndexInput input, final Collection<String> deletedFiles, final String name) {
    return new IndexInput() {
      private IndexInput in = input;

      private void checkForDeleted() throws IOException {
        if (deletedFiles.contains(name)) {
          throw new IOException("File [" + name + "] does not exist");
        }
      }

      public void skipChars(int length) throws IOException {
        checkForDeleted();
        in.skipChars(length);
      }

      public void setModifiedUTF8StringsMode() {
        in.setModifiedUTF8StringsMode();
      }

      public void close() throws IOException {
        checkForDeleted();
        in.close();
      }

      public short readShort() throws IOException {
        checkForDeleted();
        return in.readShort();
      }

      public void seek(long pos) throws IOException {
        checkForDeleted();
        in.seek(pos);
      }

      public int readInt() throws IOException {
        checkForDeleted();
        return in.readInt();
      }

      public void copyBytes(IndexOutput out, long numBytes) throws IOException {
        checkForDeleted();
        in.copyBytes(out, numBytes);
      }

      public int readVInt() throws IOException {
        checkForDeleted();
        return in.readVInt();
      }

      public String toString() {
        return in.toString();
      }

      public long readLong() throws IOException {
        checkForDeleted();
        return in.readLong();
      }

      public long readVLong() throws IOException {
        checkForDeleted();
        return in.readVLong();
      }

      public String readString() throws IOException {
        checkForDeleted();
        return in.readString();
      }

      public Object clone() {
        return super.clone();
      }

      public boolean equals(Object obj) {
        return in.equals(obj);
      }

      public long getFilePointer() {
        return in.getFilePointer();
      }

      public int hashCode() {
        return in.hashCode();
      }

      public byte readByte() throws IOException {
        checkForDeleted();
        return in.readByte();
      }

      public void readBytes(byte[] b, int offset, int len) throws IOException {
        checkForDeleted();
        in.readBytes(b, offset, len);
      }

      public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        checkForDeleted();
        in.readBytes(b, offset, len, useBuffer);
      }

      public long length() {
        return in.length();
      }

      public void readChars(char[] buffer, int start, int length) throws IOException {
        checkForDeleted();
        in.readChars(buffer, start, length);
      }

      public Map<String, String> readStringStringMap() throws IOException {
        checkForDeleted();
        return in.readStringStringMap();
      }
    };
  }
}
