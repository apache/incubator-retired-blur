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

import java.io.IOException;
import java.util.Collection;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class FencedDirectory extends Directory implements DirectoryDecorator, LastModified {

  private static final String WRITE_LOCK = "write.lock";
  private final Directory _directory;
  private final LastModified _lastModified;

  private Lock _writeLock;

  public FencedDirectory(Directory directory) {
    _directory = directory;
    if (_directory instanceof LastModified) {
      _lastModified = (LastModified) _directory;
    } else {
      _lastModified = null;
    }
  }

  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  public void deleteFile(String name) throws IOException {
    checkLock();
    _directory.deleteFile(name);
  }

  private void checkLock() throws IOException {
    if (_writeLock != null && !_writeLock.isLocked()) {
      throw new IOException("Lock [" + WRITE_LOCK + "] has been lost.");
    }
  }

  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    checkLock();
    return _directory.createOutput(name, context);
  }

  public void sync(Collection<String> names) throws IOException {
    checkLock();
    _directory.sync(names);
  }

  public IndexInput openInput(String name, IOContext context) throws IOException {
    return _directory.openInput(name, context);
  }

  public Lock makeLock(String name) {
    if (name.equals(WRITE_LOCK)) {
      if (_writeLock == null) {
        return _writeLock = _directory.makeLock(name);
      }
      return _writeLock;
    } else {
      throw new RuntimeException("Locks with name [" + name + "] not supported.");
    }
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

  public String toString() {
    return "FencedDirectory:{" + _directory.toString() + "}";
  }

  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    checkLock();
    _directory.copy(to, src, dest, context);
  }

  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    return _directory.createSlicer(name, context);
  }

  @Override
  public long getFileModified(String name) throws IOException {
    if (_directory instanceof LastModified) {
      return _lastModified.getFileModified(name);
    }
    throw new RuntimeException("Directory [] does not support last modified call.");
  }

  @Override
  public Directory getOriginalDirectory() {
    return _directory;
  }

}
