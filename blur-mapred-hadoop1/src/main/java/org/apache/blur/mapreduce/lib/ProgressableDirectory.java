package org.apache.blur.mapreduce.lib;

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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

/**
 * The {@link ProgressableDirectory} allows for progress to be recorded while
 * Lucene is blocked and merging. This prevents the Task from being killed after
 * not reporting progress because of the blocked merge.
 */
public class ProgressableDirectory extends Directory {

  private static final Log LOG = LogFactory.getLog(ProgressableDirectory.class);

  private final Directory _directory;
  private final Progressable _progressable;

  public ProgressableDirectory(Directory directory, Progressable progressable) {
    _directory = directory;
    if (progressable == null) {
      LOG.warn("Progressable is null.");
      _progressable = new Progressable() {
        @Override
        public void progress() {

        }
      };
    } else {
      _progressable = progressable;
    }
  }

  @Override
  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  @Override
  public void close() throws IOException {
    _directory.close();
  }

  private IndexInput wrapInput(String name, IndexInput openInput) {
    return new ProgressableIndexInput(name, openInput, 16384, _progressable);
  }

  private IndexOutput wrapOutput(IndexOutput createOutput) {
    return new ProgressableIndexOutput(createOutput, _progressable);
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return wrapOutput(_directory.createOutput(name, context));
  }

  @Override
  public void deleteFile(String name) throws IOException {
    _directory.deleteFile(name);
  }

  @Override
  public boolean equals(Object obj) {
    return _directory.equals(obj);
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
  public String getLockID() {
    return _directory.getLockID();
  }

  @Override
  public int hashCode() {
    return _directory.hashCode();
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
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return wrapInput(name, _directory.openInput(name, context));
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  @Override
  public String toString() {
    return _directory.toString();
  }

  static class ProgressableIndexOutput extends BufferedIndexOutput {

    private Progressable _progressable;
    private IndexOutput _indexOutput;

    public ProgressableIndexOutput(IndexOutput indexOutput, Progressable progressable) {
      _indexOutput = indexOutput;
      _progressable = progressable;
    }

    @Override
    protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
      _indexOutput.writeBytes(b, offset, len);
      _progressable.progress();
    }

    @Override
    public long length() throws IOException {
      return _indexOutput.length();
    }

    @Override
    public void close() throws IOException {
      super.close();
      _indexOutput.close();
      _progressable.progress();
    }

  }

  static class ProgressableIndexInput extends BufferedIndexInput {

    private IndexInput _indexInput;
    private final long _length;
    private Progressable _progressable;

    ProgressableIndexInput(String name, IndexInput indexInput, int buffer, Progressable progressable) {
      super("ProgressableIndexInput(" + indexInput.toString() + ")", buffer);
      _indexInput = indexInput;
      _length = indexInput.length();
      _progressable = progressable;
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      long filePointer = getFilePointer();
      if (filePointer != _indexInput.getFilePointer()) {
        _indexInput.seek(filePointer);
      }
      _indexInput.readBytes(b, offset, length);
      _progressable.progress();
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    public void close() throws IOException {
      _indexInput.close();
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public ProgressableIndexInput clone() {
      ProgressableIndexInput clone = (ProgressableIndexInput) super.clone();
      clone._indexInput = (IndexInput) _indexInput.clone();
      return clone;
    }
  }
}
