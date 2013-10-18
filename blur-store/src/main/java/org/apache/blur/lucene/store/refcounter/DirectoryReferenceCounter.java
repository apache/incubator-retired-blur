package org.apache.blur.lucene.store.refcounter;

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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class DirectoryReferenceCounter extends Directory implements DirectoryDecorator {

  private final static Log LOG = LogFactory.getLog(DirectoryReferenceCounter.class);
  private final Directory _directory;
  private final Map<String, AtomicInteger> refCounters = new ConcurrentHashMap<String, AtomicInteger>();
  private final DirectoryReferenceFileGC _gc;
  private final IndexInputCloser _closer;

  public DirectoryReferenceCounter(Directory directory, DirectoryReferenceFileGC gc, IndexInputCloser closer) {
    _directory = directory;
    _gc = gc;
    _closer = closer;
  }

  private IndexInput wrap(String name, IndexInput input) {
    AtomicInteger counter = refCounters.get(name);
    if (counter == null) {
      counter = new AtomicInteger();
      refCounters.put(name, counter);
    }
    return new RefIndexInput(input.toString(), input, counter, _closer);
  }

  public void deleteFile(String name) throws IOException {
    LOG.debug("deleteFile [{0}] being called", name);
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      _directory.deleteFile(name);
      return;
    }
    AtomicInteger counter = refCounters.get(name);
    if (counter != null && counter.get() > 0) {
      addToFileGC(name);
    } else {
      LOG.debug("Delete file [{0}]", name);
      _directory.deleteFile(name);
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      return _directory.createOutput(name, context);
    }
    LOG.debug("Create file [{0}]", name);
    AtomicInteger counter = refCounters.get(name);
    if (counter != null) {
      LOG.error("Unknown error while trying to create ref counter for [{0}] reference exists.", name);
      throw new IOException("Reference exists [" + name + "]");
    }
    refCounters.put(name, new AtomicInteger(0));
    return _directory.createOutput(name, context);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput input = _directory.openInput(name, context);
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      return input;
    }
    return wrap(name, input);
  }

  public static class RefIndexInput extends IndexInput {

    private IndexInput input;
    private AtomicInteger ref;
    private boolean closed = false;
    private IndexInputCloser closer;

    public RefIndexInput(String resourceDescription, IndexInput input, AtomicInteger ref, IndexInputCloser closer) {
      super(resourceDescription);
      this.input = input;
      this.ref = ref;
      this.closer = closer;
      ref.incrementAndGet();
      if (closer != null) {
        closer.add(this);
      }
    }

    @Override
    protected void finalize() throws Throwable {
      // Seems like not all the clones are being closed...
      if (!closed) {
        LOG.debug("[{0}] Last resort closing.", input.toString());
        close();
      }
    }

    @Override
    public RefIndexInput clone() {
      RefIndexInput refIndexInput = (RefIndexInput) super.clone();
      if (closer != null) {
        closer.add(refIndexInput);
      }
      refIndexInput.input = (IndexInput) input.clone();
      refIndexInput.ref.incrementAndGet();
      return refIndexInput;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        input.close();
        ref.decrementAndGet();
        closed = true;
      }
    }

    @Override
    public short readShort() throws IOException {
      return input.readShort();
    }

    @Override
    public int readInt() throws IOException {
      return input.readInt();
    }

    @Override
    public void seek(long pos) throws IOException {
      input.seek(pos);
    }

    @Override
    public int readVInt() throws IOException {
      return input.readVInt();
    }

    @Override
    public String toString() {
      return input.toString();
    }

    @Override
    public long readLong() throws IOException {
      return input.readLong();
    }

    @Override
    public long readVLong() throws IOException {
      return input.readVLong();
    }

    @Override
    public String readString() throws IOException {
      return input.readString();
    }

    @Override
    public long getFilePointer() {
      return input.getFilePointer();
    }

    @Override
    public byte readByte() throws IOException {
      return input.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      input.readBytes(b, offset, len);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      input.readBytes(b, offset, len, useBuffer);
    }

    @Override
    public long length() {
      return input.length();
    }

    @Override
    public Map<String, String> readStringStringMap() throws IOException {
      return input.readStringStringMap();
    }

  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  @Override
  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  @Override
  public void close() throws IOException {
    _directory.close();
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
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
  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  @Override
  public LockFactory getLockFactory() {
    return _directory.getLockFactory();
  }

  @Override
  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  @Override
  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  @Override
  public String toString() {
    return _directory.toString();
  }

  private void addToFileGC(String name) {
    if (_gc != null) {
      LOG.debug("Add file [{0}] to be GCed once refs are closed.", name);
      _gc.add(_directory, name, refCounters);
    }
  }

  @Override
  public Directory getOriginalDirectory() {
    return _directory;
  }

}