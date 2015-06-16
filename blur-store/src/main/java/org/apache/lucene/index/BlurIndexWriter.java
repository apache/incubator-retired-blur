package org.apache.lucene.index;

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
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.blur.index.ExitableReader;
import org.apache.blur.lucene.index.FencedDirectory;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

public class BlurIndexWriter extends org.apache.lucene.index.IndexWriter {

  public static class LockOwnerException extends IOException {
    private static final long serialVersionUID = -8211546713487754992L;

    public LockOwnerException(String msg) {
      super(msg);
    }
  }

  private final Lock internalLock;
  private final boolean _makeReaderExitable;

  public BlurIndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException,
      IOException {
    this(d, conf, false);
  }

  public BlurIndexWriter(Directory d, IndexWriterConfig conf, boolean makeReaderExitable) throws CorruptIndexException,
      LockObtainFailedException, IOException {
    super(fence(d), conf);
    try {
      internalLock = findInternalLock();
    } catch (Exception e) {
      throw new RuntimeException("Could not get the write lock instance.", e);
    }
    _makeReaderExitable = makeReaderExitable;
  }

  private static Directory fence(Directory directory) {
    if (directory instanceof FencedDirectory) {
      return directory;
    } else {
      return new FencedDirectory(directory);
    }
  }

  public Lock getInternalLock() {
    return internalLock;
  }

  @Override
  DirectoryReader getReader() throws IOException {
    return wrap(super.getReader());
  }

  @Override
  DirectoryReader getReader(boolean applyAllDeletes) throws IOException {
    return wrap(super.getReader(applyAllDeletes));
  }

  private DirectoryReader wrap(DirectoryReader reader) {
    if (_makeReaderExitable) {
      reader = new ExitableReader(reader);
    }
    return reader;
  }

  private Lock findInternalLock() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException {
    Field field = org.apache.lucene.index.IndexWriter.class.getDeclaredField("writeLock");
    field.setAccessible(true);
    return (Lock) field.get(this);
  }

  @Override
  protected void doAfterFlush() throws IOException {
    super.doAfterFlush();
    if (!internalLock.isLocked()) {
      throw new LockOwnerException("Lock [" + internalLock + "] no longer has write lock.");
    }
  }

  @Override
  protected void doBeforeFlush() throws IOException {
    super.doBeforeFlush();
    if (!internalLock.isLocked()) {
      throw new LockOwnerException("Lock [" + internalLock + "] no longer has write lock.");
    }
  }

  /**
   * This seems a little iffy, but basically only the writer instance itself can
   * equal itself.
   */
  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj)) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "IndexWriter with directory [" + getDirectory() + "]";
  }

}
