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
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;


public class DirectoryReferenceCounter extends Directory {

  private final static Log LOG = LogFactory.getLog(DirectoryReferenceCounter.class);
  private Directory directory;
  private Map<String, AtomicInteger> refs = new ConcurrentHashMap<String, AtomicInteger>();
  private DirectoryReferenceFileGC gc;

  public DirectoryReferenceCounter(Directory directory, DirectoryReferenceFileGC gc) {
    this.directory = directory;
    this.gc = gc;
  }

  public void deleteFile(String name) throws IOException {
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      deleteFile(name);
      return;
    }
    AtomicInteger counter = refs.get(name);
    if (counter != null && counter.get() > 0) {
      addToFileGC(name);
    } else {
      LOG.debug("Delete file [{0}]", name);
      directory.deleteFile(name);
    }
  }

  private void addToFileGC(String name) {
    if (gc != null) {
      LOG.debug("Add file [{0}] to be GCed once refs are closed.", name);
      gc.add(directory, name, refs);
    }
  }

  public IndexOutput createOutput(String name) throws IOException {
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      return directory.createOutput(name);
    }
    LOG.debug("Create file [{0}]", name);
    AtomicInteger counter = refs.get(name);
    if (counter != null) {
      LOG.error("Unknown error while trying to create ref counter for [{0}] reference exists.", name);
      throw new IOException("Reference exists [" + name + "]");
    }
    counter = new AtomicInteger(0);
    refs.put(name, counter);
    return directory.createOutput(name);
  }

  public IndexInput openInput(String name) throws IOException {
    IndexInput input = directory.openInput(name);
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      return input;
    }
    return wrap(name, input);
  }

  public IndexInput openInput(String name, int bufferSize) throws IOException {
    IndexInput input = directory.openInput(name, bufferSize);
    if (name.equals(IndexFileNames.SEGMENTS_GEN)) {
      return input;
    }
    return wrap(name, input);
  }

  private IndexInput wrap(String name, IndexInput input) {
    AtomicInteger counter = refs.get(name);
    if (counter == null) {
      counter = new AtomicInteger();
      refs.put(name, counter);
    }
    return new RefIndexInput(input, counter);
  }

  @SuppressWarnings("deprecation")
  public static class RefIndexInput extends IndexInput {

    private IndexInput input;
    private AtomicInteger ref;
    private boolean closed = false;

    public RefIndexInput(IndexInput input, AtomicInteger ref) {
      this.input = input;
      this.ref = ref;
      ref.incrementAndGet();
    }

    @Override
    protected void finalize() throws Throwable {
      // Seems like not all the clones are being closed...
      close();
    }

    public Object clone() {
      RefIndexInput ref = (RefIndexInput) super.clone();
      ref.input = (IndexInput) input.clone();
      ref.ref.incrementAndGet();
      return ref;
    }

    public void skipChars(int length) throws IOException {
      input.skipChars(length);
    }

    public void setModifiedUTF8StringsMode() {
      input.setModifiedUTF8StringsMode();
    }

    public void close() throws IOException {
      if (!closed) {
        input.close();
        ref.decrementAndGet();
        closed = true;
      }
    }

    public short readShort() throws IOException {
      return input.readShort();
    }

    public int readInt() throws IOException {
      return input.readInt();
    }

    public void seek(long pos) throws IOException {
      input.seek(pos);
    }

    public void copyBytes(IndexOutput out, long numBytes) throws IOException {
      input.copyBytes(out, numBytes);
    }

    public int readVInt() throws IOException {
      return input.readVInt();
    }

    public String toString() {
      return input.toString();
    }

    public long readLong() throws IOException {
      return input.readLong();
    }

    public long readVLong() throws IOException {
      return input.readVLong();
    }

    public String readString() throws IOException {
      return input.readString();
    }

    public long getFilePointer() {
      return input.getFilePointer();
    }

    public byte readByte() throws IOException {
      return input.readByte();
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
      input.readBytes(b, offset, len);
    }

    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
      input.readBytes(b, offset, len, useBuffer);
    }

    public long length() {
      return input.length();
    }

    public void readChars(char[] buffer, int start, int length) throws IOException {
      input.readChars(buffer, start, length);
    }

    public Map<String, String> readStringStringMap() throws IOException {
      return input.readStringStringMap();
    }

  }

  @SuppressWarnings("deprecation")
  public void touchFile(String name) throws IOException {
    directory.touchFile(name);
  }

  @SuppressWarnings("deprecation")
  public void sync(String name) throws IOException {
    directory.sync(name);
  }

  public void sync(Collection<String> names) throws IOException {
    directory.sync(names);
  }

  public void clearLock(String name) throws IOException {
    directory.clearLock(name);
  }

  public void close() throws IOException {
    directory.close();
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    directory.setLockFactory(lockFactory);
  }

  public String getLockID() {
    return directory.getLockID();
  }

  public void copy(Directory to, String src, String dest) throws IOException {
    directory.copy(to, src, dest);
  }

  public boolean fileExists(String name) throws IOException {
    return directory.fileExists(name);
  }

  @SuppressWarnings("deprecation")
  public long fileModified(String name) throws IOException {
    return directory.fileModified(name);
  }

  public long fileLength(String name) throws IOException {
    return directory.fileLength(name);
  }

  public LockFactory getLockFactory() {
    return directory.getLockFactory();
  }

  public String[] listAll() throws IOException {
    return directory.listAll();
  }

  public Lock makeLock(String name) {
    return directory.makeLock(name);
  }

  public String toString() {
    return directory.toString();
  }

}