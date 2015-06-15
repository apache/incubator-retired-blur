package org.apache.blur.store;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.blur.index.IndexDeletionPolicyReader;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.buffer.BufferStore;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class BaseDirectoryTestSuite {
  protected static final File TMPDIR = new File(System.getProperty("blur.tmp.dir",
      "./target/tmp/BaseDirectoryTestSuite"));

  protected static final int MAX_NUMBER_OF_WRITES = 10000;
  protected static final int MIN_FILE_SIZE = 100;
  protected static final int MAX_FILE_SIZE = 100000;
  protected static final int MIN_BUFFER_SIZE = 1;
  protected static final int MAX_BUFFER_SIZE = 5000;
  protected static final int MAX_NUMBER_OF_READS = 10000;
  protected Directory directory;
  protected File file;
  protected File fileControl;
  protected long seed;
  protected Random random;

  @Before
  public void setUp() throws IOException {
    BufferStore.initNewBuffer(1024, 1024 * 128);
    BufferStore.initNewBuffer(8192, 8192 * 128);

    file = new File(TMPDIR, "hdfsdirectorytest");
    fileControl = new File(TMPDIR, "hdfsdirectorytest-control");
    rm(file);
    rm(fileControl);
    seed = new Random().nextLong();
    System.out.println("Seed [" + seed + "]");
    seed = 4975561783806198199L;
    random = new Random(seed);
    directory = setupDirectory();
  }

  @After
  public void tearDown() throws IOException {
    print(file, "");
    close();
  }

  protected abstract void close() throws IOException;

  private void print(File f, String buf) {
    if (f.isDirectory()) {
      System.out.println(buf + "\\" + f.getName());
      for (File fl : f.listFiles()) {
        if (fl.getName().startsWith(".")) {
          continue;
        }
        print(fl, buf + " ");
      }
    } else {
      System.out.println(buf + f.getName() + " " + f.length());
    }
  }

  protected abstract Directory setupDirectory() throws IOException;

  @Test
  public void testWritingAndReadingAFile() throws IOException {

    IndexOutput output = directory.createOutput("testing.test", IOContext.DEFAULT);
    output.writeInt(12345);
    output.flush();
    output.close();

    IndexInput input = directory.openInput("testing.test", IOContext.DEFAULT);
    assertEquals(12345, input.readInt());
    input.close();

    String[] listAll = directory.listAll();
    assertEquals(1, listAll.length);
    assertEquals("testing.test", listAll[0]);

    assertEquals(4, directory.fileLength("testing.test"));

    IndexInput input1 = directory.openInput("testing.test", IOContext.DEFAULT);

    IndexInput input2 = (IndexInput) input1.clone();
    assertEquals(12345, input2.readInt());
    input2.close();

    assertEquals(12345, input1.readInt());
    input1.close();

    assertFalse(directory.fileExists("testing.test.other"));
    assertTrue(directory.fileExists("testing.test"));
    directory.deleteFile("testing.test");
    assertFalse(directory.fileExists("testing.test"));
  }

  @Test
  public void testEOF() throws IOException {
    Directory fsDir = new RAMDirectory();
    String name = "test.eof";
    createFile(name, fsDir, directory);
    long fsLength = fsDir.fileLength(name);
    long hdfsLength = directory.fileLength(name);
    assertEquals(fsLength, hdfsLength);
    testEof(name, fsDir, fsLength);
    testEof(name, directory, hdfsLength);
  }

  private void testEof(String name, Directory directory, long length) throws IOException {
    IndexInput input = directory.openInput(name, IOContext.DEFAULT);
    try {
      input.seek(length);
      input.readByte();
      fail("should throw eof");
    } catch (IOException e) {
    }
  }

  @Test
  public void testWrites() throws IOException {
    int i = 0;
    try {
      Set<String> names = new HashSet<String>();
      for (; i < 10; i++) {
        Directory fsDir = new RAMDirectory();
        String name = getName();
        System.out.println("Working on pass [" + i + "] seed [" + seed + "] contains [" + names.contains(name) + "]");
        names.add(name);
        createFile(name, fsDir, directory);
        assertInputsEquals(name, fsDir, directory);
        fsDir.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed with seed [" + seed + "] on pass [" + i + "]");
    }
  }

  @Test
  public void testCreateIndex() throws IOException {
    long s = System.nanoTime();
    IndexWriterConfig conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, new KeywordAnalyzer());
    IndexDeletionPolicyReader indexDeletionPolicy = new IndexDeletionPolicyReader(
        new KeepOnlyLastCommitDeletionPolicy());
    conf.setIndexDeletionPolicy(indexDeletionPolicy);
    FSDirectory control = FSDirectory.open(fileControl);
    Directory dir = getControlDir(control, directory);
    // The serial merge scheduler can be useful for debugging.
    // conf.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = 1000;
    DirectoryReader reader = null;
    long gen = 0;
    for (int i = 0; i < 100; i++) {
      if (reader == null) {
        reader = DirectoryReader.open(writer, true);
        gen = reader.getIndexCommit().getGeneration();
        indexDeletionPolicy.register(gen);
      } else {
        DirectoryReader old = reader;
        reader = DirectoryReader.openIfChanged(old, writer, true);
        if (reader == null) {
          reader = old;
        } else {
          long newGen = reader.getIndexCommit().getGeneration();
          indexDeletionPolicy.register(newGen);
          indexDeletionPolicy.unregister(gen);
          old.close();
          gen = newGen;
        }
      }
      assertEquals(i * numDocs, reader.numDocs());
      IndexSearcher searcher = new IndexSearcher(reader);
      NumericRangeQuery<Integer> query = NumericRangeQuery.newIntRange("id", 42, 42, true, true);
      TopDocs topDocs = searcher.search(query, 10);
      assertEquals(i, topDocs.totalHits);
      addDocuments(writer, numDocs);
    }
    writer.close(false);
    reader.close();
    long e = System.nanoTime();
    System.out.println("Total time [" + (e - s) / 1000000.0 + " ms]");
  }

  private void addDocuments(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      writer.addDocument(getDoc(i));
    }
  }

  private Document getDoc(int i) {
    Document document = new Document();
    document.add(new IntField("id", i, Store.YES));
    return document;
  }

  private void assertInputsEquals(String name, Directory fsDir, Directory hdfs) throws IOException {
    int reads = random.nextInt(MAX_NUMBER_OF_READS);
    IndexInput fsInput = fsDir.openInput(name, IOContext.DEFAULT);
    IndexInput hdfsInput = hdfs.openInput(name, IOContext.DEFAULT);
    assertEquals(fsInput.length(), hdfsInput.length());
    int fileLength = (int) fsInput.length();
    for (int i = 0; i < reads; i++) {
      byte[] fsBuf = new byte[random.nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE, fileLength)) + MIN_BUFFER_SIZE];
      byte[] hdfsBuf = new byte[fsBuf.length];
      int offset = random.nextInt(fsBuf.length);
      int length = random.nextInt(fsBuf.length - offset);
      int pos = random.nextInt(fileLength - length);
      fsInput.seek(pos);
      fsInput.readBytes(fsBuf, offset, length);
      hdfsInput.seek(pos);
      hdfsInput.readBytes(hdfsBuf, offset, length);
      for (int f = offset; f < length; f++) {
        if (fsBuf[f] != hdfsBuf[f]) {
          fail();
        }
      }
    }
    fsInput.close();
    hdfsInput.close();
  }

  private void createFile(String name, Directory fsDir, Directory hdfs) throws IOException {
    int writes = random.nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random.nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name, IOContext.DEFAULT);
    fsOutput.setLength(fileLength);
    IndexOutput hdfsOutput = hdfs.createOutput(name, IOContext.DEFAULT);
    hdfsOutput.setLength(fileLength);
    for (int i = 0; i < writes; i++) {
      byte[] buf = new byte[random.nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE, fileLength)) + MIN_BUFFER_SIZE];
      random.nextBytes(buf);
      int offset = random.nextInt(buf.length);
      int length = random.nextInt(buf.length - offset);
      fsOutput.writeBytes(buf, offset, length);
      hdfsOutput.writeBytes(buf, offset, length);
    }
    fsOutput.close();
    hdfsOutput.close();
  }

  private String getName() {
    return Long.toString(Math.abs(random.nextLong()));
  }

  public static void rm(File file) {
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

  public static Directory wrapLastModified(Directory dir) {
    return new DirectoryLastModified(dir);
  }

  public static class DirectoryLastModified extends Directory implements LastModified {

    private Directory _directory;

    public DirectoryLastModified(Directory dir) {
      _directory = dir;
    }

    @Override
    public long getFileModified(String name) throws IOException {
      if (_directory instanceof FSDirectory) {
        File fileDir = ((FSDirectory) _directory).getDirectory();
        return new File(fileDir, name).lastModified();
      }
      throw new RuntimeException("not impl");
    }

    @Override
    public String[] listAll() throws IOException {
      return _directory.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
      _directory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
      return _directory.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      return _directory.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
      _directory.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return _directory.openInput(name, context);
    }

    @Override
    public Lock makeLock(String name) {
      return _directory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
      _directory.clearLock(name);
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
      _directory.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
      return _directory.getLockFactory();
    }

    @Override
    public String getLockID() {
      return _directory.getLockID();
    }

    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
      _directory.copy(to, src, dest, context);
    }

    @Override
    public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
      return _directory.createSlicer(name, context);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
      return _directory.fileExists(name);
    }

    @Override
    public void close() throws IOException {
      _directory.close();
    }
  }

  private Directory getControlDir(final Directory control, final Directory test) {
    return new Directory() {

      @Override
      public Lock makeLock(String name) {
        return control.makeLock(name);
      }

      @Override
      public void clearLock(String name) throws IOException {
        control.clearLock(name);
      }

      @Override
      public void setLockFactory(LockFactory lockFactory) throws IOException {
        control.setLockFactory(lockFactory);
      }

      @Override
      public LockFactory getLockFactory() {
        return control.getLockFactory();
      }

      @Override
      public String getLockID() {
        return control.getLockID();
      }

      @Override
      public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        control.copy(to, src, dest, context);
      }

      @Override
      public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
        return control.createSlicer(name, context);
      }

      @Override
      public IndexOutput createOutput(final String name, IOContext context) throws IOException {
        final IndexOutput testOutput = test.createOutput(name, context);
        final IndexOutput controlOutput = control.createOutput(name, context);
        return new IndexOutput() {

          @Override
          public void flush() throws IOException {
            testOutput.flush();
            controlOutput.flush();
          }

          @Override
          public void close() throws IOException {
            testOutput.close();
            controlOutput.close();
          }

          @Override
          public long getFilePointer() {
            long filePointer = testOutput.getFilePointer();
            long controlFilePointer = controlOutput.getFilePointer();
            if (controlFilePointer != filePointer) {
              System.err.println("Output Name [" + name + "] with filePointer [" + filePointer
                  + "] and control filePointer [" + controlFilePointer + "] does not match");
            }
            return filePointer;
          }

          @SuppressWarnings("deprecation")
          @Override
          public void seek(long pos) throws IOException {
            testOutput.seek(pos);
            controlOutput.seek(pos);
          }

          @Override
          public long length() throws IOException {
            long length = testOutput.length();
            long controlLength = controlOutput.length();
            if (controlLength != length) {
              System.err.println("Ouput Name [" + name + "] with length [" + length + "] and control length ["
                  + controlLength + "] does not match");
            }
            return length;
          }

          @Override
          public void writeByte(byte b) throws IOException {
            testOutput.writeByte(b);
            controlOutput.writeByte(b);
          }

          @Override
          public void writeBytes(byte[] b, int offset, int length) throws IOException {
            testOutput.writeBytes(b, offset, length);
            controlOutput.writeBytes(b, offset, length);
          }

        };
      }

      @Override
      public IndexInput openInput(final String name, IOContext context) throws IOException {
        final IndexInput testInput = test.openInput(name, context);
        final IndexInput controlInput = control.openInput(name, context);
        return new IndexInputCompare(name, testInput, controlInput);
      }

      @Override
      public String[] listAll() throws IOException {
        return test.listAll();
      }

      @Override
      public boolean fileExists(String name) throws IOException {
        return test.fileExists(name);
      }

      @Override
      public void deleteFile(String name) throws IOException {
        test.deleteFile(name);
        control.deleteFile(name);
      }

      @Override
      public long fileLength(String name) throws IOException {
        long fileLength = test.fileLength(name);
        long controlFileLength = control.fileLength(name);
        if (controlFileLength != fileLength) {
          System.err.println("Input Name [" + name + "] with length [" + fileLength + "] and control length ["
              + controlFileLength + "] does not match");
        }
        return fileLength;
      }

      @Override
      public void sync(Collection<String> names) throws IOException {
        test.sync(names);
        test.sync(names);
      }

      @Override
      public void close() throws IOException {
        test.close();
        control.close();
      }
    };
  }

  static class IndexInputCompare extends IndexInput {

    IndexInput testInput;
    IndexInput controlInput;
    String name;

    protected IndexInputCompare(String name, IndexInput testInput, IndexInput controlInput) {
      super(name);
      this.name = name;
      this.testInput = testInput;
      this.controlInput = controlInput;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      breakPoint();
      long filePointer = getFilePointer();
      testInput.readBytes(b, offset, len);
      byte[] newbuf = new byte[b.length];
      controlInput.readBytes(newbuf, offset, len);
      compare(filePointer, b, newbuf, offset, len);
      getFilePointer();
    }

    private void breakPoint() {
      // if (name.equals("_1e_Lucene41_0.doc")) {
      // if (controlInput.getFilePointer() > 123435) {
      // System.out.println("break [" + controlInput.getFilePointer() + "] [" +
      // controlInput.length() + "]");
      // }
      // }
    }

    @Override
    public byte readByte() throws IOException {
      breakPoint();
      long length = controlInput.length();
      long filePointer = getFilePointer();
      byte readByte = testInput.readByte();
      byte controlReadByte = controlInput.readByte();
      if (readByte != controlReadByte) {
        System.err.println("Input Name [" + name + "] at filePointer [" + filePointer + "] byte [" + readByte
            + "] does not match byte [" + controlReadByte + "] with length [" + length + "]");
        throw new RuntimeException("Input Name [" + name + "] at filePointer [" + filePointer + "] byte [" + readByte
            + "] does not match byte [" + controlReadByte + "] with length [" + length + "]");
      }
      getFilePointer();
      return readByte;
    }

    private void compare(long filePointer, byte[] b1, byte[] b2, int offset, int len) {
      int length = offset + len;
      for (int i = offset; i < length; i++) {
        if (b1[i] != b2[i]) {
          System.err.println("Input Name [" + name + "] with at filePointer [" + filePointer + "] b1 [" + b1
              + "] does not match b2 [" + b2 + "]");
          throw new RuntimeException("Input Name [" + name + "] with at filePointer [" + filePointer + "] b1 [" + b1
              + "] does not match b2 [" + b2 + "]");
        }
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      breakPoint();
      getFilePointer();
      testInput.seek(pos);
      controlInput.seek(pos);
      getFilePointer();
    }

    @Override
    public long length() {
      breakPoint();
      long length = testInput.length();
      long controlLength = controlInput.length();
      if (length != controlLength) {
        System.err.println("Input Name [" + name + "] with length [" + length + "] and control length ["
            + controlLength + "] does not match");
        throw new RuntimeException("Input Name [" + name + "] with length [" + length + "] and control length ["
            + controlLength + "] does not match");
      }
      return length;
    }

    @Override
    public long getFilePointer() {
      long filePointer = testInput.getFilePointer();
      long controlFilePointer = controlInput.getFilePointer();
      if (filePointer != controlFilePointer) {
        System.err.println("Input Name [" + name + "] with filePointer [" + filePointer + "] and control filePointer ["
            + controlFilePointer + "] does not match");
        throw new RuntimeException("Input Name [" + name + "] with filePointer [" + filePointer
            + "] and control filePointer [" + controlFilePointer + "] does not match");
      }
      return filePointer;
    }

    @Override
    public void close() throws IOException {
      controlInput.close();
      testInput.close();
    }

    @Override
    public IndexInput clone() {
      IndexInputCompare clone = (IndexInputCompare) super.clone();
      clone.controlInput = controlInput.clone();
      clone.testInput = testInput.clone();
      return clone;
    }

  }

  public int numberBetween(int min, int max) {
    return random.nextInt(max - min) + min;
  }
}
