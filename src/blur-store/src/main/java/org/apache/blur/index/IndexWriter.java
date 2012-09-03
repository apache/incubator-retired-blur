package org.apache.blur.index;

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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

public class IndexWriter extends org.apache.lucene.index.IndexWriter {

  private Lock internalLock;

  public IndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException, IOException {
    super(d, conf);
    try {
      internalLock = getInternalLock();
    } catch (Exception e) {
      throw new RuntimeException("Could not get the write lock instance.", e);
    }
  }

  private Lock getInternalLock() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    Field field = org.apache.lucene.index.IndexWriter.class.getDeclaredField("writeLock");
    field.setAccessible(true);
    return (Lock) field.get(this);
  }

  @Override
  protected void doAfterFlush() throws IOException {
    super.doAfterFlush();
    if (!internalLock.isLocked()) {
      throw new IOException("Lock [" + internalLock + "] no longer has write lock.");
    }
  }

  @Override
  protected void doBeforeFlush() throws IOException {
    super.doBeforeFlush();
    if (!internalLock.isLocked()) {
      throw new IOException("Lock [" + internalLock + "] no longer has write lock.");
    }
  }

}
