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
package org.apache.blur.store.hdfs_v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FilterInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.lucene.util.BytesRef;

public class HdfsKeyValueStore implements Store {

  // private final Log LOG = LogFactory.getLog(HdfsKeyValueStore.class);

  static enum OperationType {
    PUT, DELETE
  }

  static class Operation implements Writable {

    OperationType type;
    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    @Override
    public void write(DataOutput out) throws IOException {
      if (type == OperationType.DELETE) {
        out.write(0);
        key.write(out);
      } else if (type == OperationType.PUT) {
        out.write(1);
        key.write(out);
        value.write(out);
      } else {
        throw new RuntimeException("Not supported [" + type + "]");
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      byte b = in.readByte();
      switch (b) {
      case 0:
        type = OperationType.DELETE;
        key.readFields(in);
        return;
      case 1:
        type = OperationType.PUT;
        key.readFields(in);
        value.readFields(in);
        return;
      default:
        throw new RuntimeException("Not supported [" + b + "]");
      }
    }

  }

  private static final Comparator<BytesRef> COMP = new Comparator<BytesRef>() {
    @Override
    public int compare(BytesRef b1, BytesRef b2) {
      return WritableComparator.compareBytes(b1.bytes, b1.offset, b1.length, b2.bytes, b2.offset, b2.length);
    }
  };

  private final ConcurrentNavigableMap<BytesRef, BytesRef> _pointers = new ConcurrentSkipListMap<BytesRef, BytesRef>(
      COMP);
  private final Configuration _configuration;
  private final Path _path;
  private final ReentrantReadWriteLock _readWriteLock;
  private final AtomicReference<SortedSet<FileStatus>> _fileStatus = new AtomicReference<SortedSet<FileStatus>>();
  private final FileSystem _fileSystem;
  private final AtomicLong _currentFileCounter = new AtomicLong();
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private FSDataOutputStream _output;
  private Path _outputPath;
  private boolean _isClosed;

  public HdfsKeyValueStore(Configuration configuration, Path path) throws IOException {
    _configuration = configuration;
    _path = path;
    _configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
    _fileSystem = FileSystem.get(_path.toUri(), _configuration);
    _readWriteLock = new ReentrantReadWriteLock();
    _writeLock = _readWriteLock.writeLock();
    _readLock = _readWriteLock.readLock();
    _fileStatus.set(getList(_path));
    if (!_fileStatus.get().isEmpty()) {
      _currentFileCounter.set(Long.parseLong(_fileStatus.get().last().getPath().getName()));
    }
    loadIndexes();
    openWriter();
  }

  @Override
  public void sync() throws IOException {
    ensureOpen();
    _writeLock.lock();
    try {
      syncInternal();
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public Iterable<Entry<BytesRef, BytesRef>> scan(BytesRef key) throws IOException {
    ensureOpen();
    // to do
    return null;
  }

  @Override
  public void put(BytesRef key, BytesRef value) throws IOException {
    ensureOpen();
    if (value == null) {
      delete(key);
      return;
    }
    _writeLock.lock();
    try {
      Operation op = getPutOperation(OperationType.PUT, key, value);
      write(op);
      _pointers.put(BytesRef.deepCopyOf(key), BytesRef.deepCopyOf(value));
    } finally {
      _writeLock.unlock();
    }
  }

  private void write(Operation op) throws IOException {
    op.write(_output);
  }

  private Operation getPutOperation(OperationType put, BytesRef key, BytesRef value) {
    Operation operation = new Operation();
    operation.type = put;
    operation.key.set(key.bytes, key.offset, key.length);
    operation.value.set(value.bytes, value.offset, value.length);
    return operation;
  }

  private Operation getDeleteOperation(OperationType delete, BytesRef key) {
    Operation operation = new Operation();
    operation.type = delete;
    operation.key.set(key.bytes, key.offset, key.length);
    return operation;
  }

  @Override
  public boolean get(BytesRef key, BytesRef value) throws IOException {
    ensureOpen();
    _readLock.lock();
    try {
      BytesRef internalValue = _pointers.get(key);
      if (internalValue == null) {
        return false;
      }
      value.copyBytes(internalValue);
      return true;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void delete(BytesRef key) throws IOException {
    ensureOpen();
    _writeLock.lock();
    try {
      Operation op = getDeleteOperation(OperationType.DELETE, key);
      write(op);
      _pointers.remove(key);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (_isClosed) {
      _writeLock.lock();
      try {
        syncInternal();
        _output.close();
        _fileSystem.close();
        _isClosed = true;
      } finally {
        _writeLock.unlock();
      }
    }
  }

  private void openWriter() throws IOException {
    long nextSegment = _currentFileCounter.incrementAndGet();
    String name = buffer(nextSegment);
    _outputPath = new Path(_path, name);
    _output = _fileSystem.create(_outputPath, false);
  }

  private String buffer(long number) {
    String s = Long.toString(number);
    StringBuilder builder = new StringBuilder();
    for (int i = s.length(); i < 12; i++) {
      builder.append('0');
    }
    return builder.append(s).toString();
  }

  private void loadIndexes() throws IOException {
    for (FileStatus fileStatus : _fileStatus.get()) {
      loadIndex(fileStatus.getPath());
    }
  }

  private void ensureOpen() throws IOException {
    if (_isClosed) {
      throw new IOException("Already closed.");
    }
  }

  private long getFileLength(Path path, FSDataInputStream inputStream) throws IOException {
    FileStatus fileStatus = _fileSystem.getFileStatus(path);
    DFSInputStream dfs = getDFS(inputStream);
    return Math.max(dfs.getFileLength(), fileStatus.getLen());
  }

  private void syncInternal() throws IOException {
    _output.flush();
    _output.sync();
  }

  private void loadIndex(Path path) throws IOException {
    FSDataInputStream inputStream = _fileSystem.open(path);
    long fileLength = getFileLength(path, inputStream);
    Operation operation = new Operation();
    try {
      while (inputStream.getPos() < fileLength) {
        try {
          operation.readFields(inputStream);
        } catch (IOException e) {
          // End of sync point found
          return;
        }
        loadIndex(operation);
      }
    } finally {
      inputStream.close();
    }
  }

  private void loadIndex(Operation operation) {
    switch (operation.type) {
    case PUT:
      _pointers.put(BytesRef.deepCopyOf(getKey(operation.key)), BytesRef.deepCopyOf(getKey(operation.value)));
      return;
    case DELETE:
      _pointers.remove(getKey(operation.key));
      return;
    default:
      throw new RuntimeException("Not supported [" + operation.type + "]");
    }
  }

  private BytesRef getKey(BytesWritable key) {
    return new BytesRef(key.getBytes(), 0, key.getLength());
  }

  private SortedSet<FileStatus> getList(Path p) throws IOException {
    FileStatus[] listStatus = _fileSystem.listStatus(p);
    if (listStatus == null) {
      return new TreeSet<FileStatus>();
    }
    return new TreeSet<FileStatus>(Arrays.asList(listStatus));
  }

  private static DFSInputStream getDFS(FSDataInputStream inputStream) throws IOException {
    try {
      Field field = FilterInputStream.class.getDeclaredField("in");
      field.setAccessible(true);
      return (DFSInputStream) field.get(inputStream);
    } catch (NoSuchFieldException e) {
      throw new IOException(e);
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }
}
