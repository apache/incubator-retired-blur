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
package org.apache.blur.lucene.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.store.hdfs.BlurLockFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BlurIndexWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class BlurIndexWriterTest {

  private Configuration _configuration = new Configuration();
  private final Object lock = new Object();

  @Test
  public void testIndexRelocationFencing() throws CorruptIndexException, LockObtainFailedException, IOException,
      InterruptedException {
    final AtomicBoolean fail1 = new AtomicBoolean();
    final AtomicBoolean fail2 = new AtomicBoolean();
    final Path hdfsDirPath = new Path(toUri("./target/tmp/BlurIndexWriterTest"));
    final Directory directory = new RAMDirectory();
    Thread thread1 = new Thread(new Runnable() {
      @Override
      public void run() {
        BlurIndexWriter writer = null;
        try {
          BlurLockFactory blurLockFactory = new BlurLockFactory(_configuration, hdfsDirPath, "node1", "1");
          directory.setLockFactory(blurLockFactory);
          IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
          conf.setInfoStream(getInfoStream());
          writer = new BlurIndexWriter(directory, conf);
          writer.addIndexes(addDir("1"));
          waitToLooseLock();
          writer.prepareCommit();
          fail1.set(true);
        } catch (IOException e) {
          e.printStackTrace();
          if (writer != null) {
            try {
              writer.rollback();
            } catch (IOException e1) {
              e1.printStackTrace();
            }
          }
          if (writer != null) {
            try {
              writer.close();
            } catch (IOException e1) {
              e1.printStackTrace();
            }
          }
        }
      }
    });

    Thread thread2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          waitForDirInThread1ToBeAdded(directory);
          BlurLockFactory blurLockFactory = new BlurLockFactory(_configuration, hdfsDirPath, "node2", "2");
          directory.setLockFactory(blurLockFactory);
          IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
          conf.setInfoStream(getInfoStream());
          BlurIndexWriter writer = new BlurIndexWriter(directory, conf);
          obtainLock();
          writer.addIndexes(addDir("2"));
          writer.commit();
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
          fail2.set(true);
        }
      }

    });
    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    if (fail1.get()) {
      fail();
    }

    if (fail2.get()) {
      fail();
    }

    DirectoryReader reader = DirectoryReader.open(directory);
    List<AtomicReaderContext> leaves = reader.leaves();
    assertEquals(leaves.size(), 1);
    assertEquals(reader.numDocs(), 1);
    Document document = reader.document(0);
    assertEquals("2", document.get("f"));
    reader.close();
  }

  protected InfoStream getInfoStream() {
    return new InfoStream() {

      @Override
      public void close() throws IOException {
      }

      @Override
      public void message(String component, String message) {
        System.out.println("Thread [" + Thread.currentThread().getName() + "] Comp [" + component + "] Message ["
            + message + "]");
      }

      @Override
      public boolean isEnabled(String component) {
        return false;
      }
    };
  }

  private void waitForDirInThread1ToBeAdded(Directory directory) throws IOException {
    while (true) {
      String[] listAll = directory.listAll();
      if (Arrays.asList(listAll).contains("_0.fnm")) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  protected void waitToLooseLock() throws IOException {
    synchronized (lock) {
      try {
        lock.wait();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  private void obtainLock() {
    synchronized (lock) {
      lock.notify();
    }
  }

  private Directory addDir(String v) throws IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory, config);
    writer.addDocument(getDoc(v));
    writer.close();

    return directory;
  }

  private Iterable<? extends IndexableField> getDoc(String v) {
    Document document = new Document();
    document.add(new StringField("f", v, Store.YES));
    return document;
  }

  private URI toUri(String f) {
    return new File(f).getAbsoluteFile().toURI();
  }

}
