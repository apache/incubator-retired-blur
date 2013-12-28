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
package org.apache.blur.lucene.warmup;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class SlowAccessDirectory extends Directory {
  private final Directory _directory;

  public static AtomicLong _reads = new AtomicLong();

  static class SlowAccessIndexInput extends BufferedIndexInput {

    private IndexInput _indexInput;

    protected SlowAccessIndexInput(IndexInput indexInput) {
      super(indexInput.toString(), 8192);
      _indexInput = indexInput;
    }

    private void delay() throws IOException {
      synchronized (this) {
        try {
          wait(1);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    }

    public void close() throws IOException {
      _indexInput.close();
    }

    public long length() {
      return _indexInput.length();
    }

    @Override
    public SlowAccessIndexInput clone() {
      SlowAccessIndexInput clone = (SlowAccessIndexInput) super.clone();
      clone._indexInput = _indexInput.clone();
      return clone;
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      delay();
      _reads.incrementAndGet();
      long filePointer = getFilePointer();
      _indexInput.seek(filePointer);
      _indexInput.readBytes(b, offset, length);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

  }

  public SlowAccessDirectory(Directory directory) {
    _directory = directory;
  }

  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  public void deleteFile(String name) throws IOException {
    _directory.deleteFile(name);
  }

  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return _directory.createOutput(name, context);
  }

  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new SlowAccessIndexInput(_directory.openInput(name, context));
  }

  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  public void close() throws IOException {
    _directory.close();
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  public LockFactory getLockFactory() {
    return _directory.getLockFactory();
  }

  public String getLockID() {
    return _directory.getLockID();
  }

  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    _directory.copy(to, src, dest, context);
  }

  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    return _directory.createSlicer(name, context);
  }
}
