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

import static org.apache.blur.metrics.MetricsConstants.HDFS_KV;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.lucene.util.BytesRef;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class HdfsKeyValueStore implements Store {

  private static final String UTF_8 = "UTF-8";
  private static final String BLUR_KEY_VALUE = "blur_key_value";
  private static final String IN = "in";
  private static final String GET_FILE_LENGTH = "getFileLength";
  private static final int DEFAULT_MAX = 64 * 1024 * 1024;
  private static final Log LOG = LogFactory.getLog(HdfsKeyValueStore.class);
  private static final byte[] MAGIC;
  private static final int VERSION = 1;
  private static final long MAX_OPEN_FOR_WRITING = TimeUnit.MINUTES.toMillis(1);
  private static final long DAEMON_POLL_TIME = TimeUnit.SECONDS.toMillis(5);
  private static final int VERSION_LENGTH = 4;

  static {
    try {
      MAGIC = BLUR_KEY_VALUE.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

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

  static class Value {
    Value(BytesRef bytesRef, Path path) {
      _bytesRef = bytesRef;
      _path = path;
    }

    BytesRef _bytesRef;
    Path _path;
  }

  private final ConcurrentNavigableMap<BytesRef, Value> _pointers = new ConcurrentSkipListMap<BytesRef, Value>(COMP);
  private final Path _path;
  private final ReentrantReadWriteLock _readWriteLock;
  private final AtomicReference<SortedSet<FileStatus>> _fileStatus = new AtomicReference<SortedSet<FileStatus>>();
  private final FileSystem _fileSystem;
  private final AtomicLong _currentFileCounter = new AtomicLong();
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final AtomicLong _size = new AtomicLong();
  private final long _maxAmountAllowedPerFile;
  private final TimerTask _idleLogTimerTask;
  private final TimerTask _oldFileCleanerTimerTask;
  private final AtomicLong _lastWrite = new AtomicLong();
  private final Timer _hdfsKeyValueTimer;

  private FSDataOutputStream _output;
  private Path _outputPath;
  private boolean _isClosed;

  public HdfsKeyValueStore(Timer hdfsKeyValueTimer, Configuration configuration, Path path) throws IOException {
    this(hdfsKeyValueTimer, configuration, path, DEFAULT_MAX);
  }

  public HdfsKeyValueStore(Timer hdfsKeyValueTimer, Configuration configuration, Path path, long maxAmountAllowedPerFile)
      throws IOException {
    _maxAmountAllowedPerFile = maxAmountAllowedPerFile;
    _path = path;
    _fileSystem = _path.getFileSystem(configuration);
    _fileSystem.mkdirs(_path);
    _readWriteLock = new ReentrantReadWriteLock();
    _writeLock = _readWriteLock.writeLock();
    _readLock = _readWriteLock.readLock();
    _fileStatus.set(getSortedSet(_path));
    if (!_fileStatus.get().isEmpty()) {
      _currentFileCounter.set(Long.parseLong(_fileStatus.get().last().getPath().getName()));
    }
    removeAnyTruncatedFiles();
    loadIndexes();
    cleanupOldFiles();
    _idleLogTimerTask = getIdleLogTimer();
    _oldFileCleanerTimerTask = getOldFileCleanerTimer();
    _hdfsKeyValueTimer = hdfsKeyValueTimer;
    _hdfsKeyValueTimer.schedule(_idleLogTimerTask, DAEMON_POLL_TIME, DAEMON_POLL_TIME);
    _hdfsKeyValueTimer.schedule(_oldFileCleanerTimerTask, DAEMON_POLL_TIME, DAEMON_POLL_TIME);
    // Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, HDFS_KV, SIZE,
    // path.getParent().toString()), new Gauge<Long>() {
    // @Override
    // public Long value() {
    // return _size.get();
    // }
    // });
  }

  private void removeAnyTruncatedFiles() throws IOException {
    for (FileStatus fileStatus : _fileStatus.get()) {
      Path path = fileStatus.getPath();
      FSDataInputStream inputStream = _fileSystem.open(path);
      long len = getFileLength(path, inputStream);
      inputStream.close();
      if (len < MAGIC.length + VERSION_LENGTH) {
        // Remove invalid file
        LOG.warn("Removing file [{0}] because length of [{1}] is less than MAGIC plus version length of [{2}]", path,
            len, MAGIC.length + VERSION_LENGTH);
        _fileSystem.delete(path, false);
      }
    }
  }

  private TimerTask getOldFileCleanerTimer() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          cleanupOldFiles();
        } catch (IOException e) {
          LOG.error("Unknown error while trying to clean up old files.", e);
        }
      }
    };
  }

  private TimerTask getIdleLogTimer() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          closeLogFileIfIdle();
        } catch (IOException e) {
          LOG.error("Unknown error while trying to close output file.", e);
        }
      }

    };
  }

  @Override
  public void sync() throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      syncInternal();
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public Iterable<Entry<BytesRef, BytesRef>> scan(BytesRef key) throws IOException {
    ensureOpen();
    if (key == null) {
      key = _pointers.firstKey();
    }
    ConcurrentNavigableMap<BytesRef, Value> tailMap = _pointers.tailMap(key, true);
    final Set<Entry<BytesRef, Value>> entrySet = tailMap.entrySet();
    return new Iterable<Entry<BytesRef, BytesRef>>() {
      @Override
      public Iterator<Entry<BytesRef, BytesRef>> iterator() {
        final Iterator<Entry<BytesRef, Value>> iterator = entrySet.iterator();
        return new Iterator<Entry<BytesRef, BytesRef>>() {

          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Entry<BytesRef, BytesRef> next() {
            final Entry<BytesRef, Value> e = iterator.next();
            return new Entry<BytesRef, BytesRef>() {

              @Override
              public BytesRef setValue(BytesRef value) {
                throw new RuntimeException("Read only.");
              }

              @Override
              public BytesRef getValue() {
                return e.getValue()._bytesRef;
              }

              @Override
              public BytesRef getKey() {
                return e.getKey();
              }
            };
          }

          @Override
          public void remove() {
            throw new RuntimeException("Read only.");
          }
        };
      }
    };
  }

  @Override
  public void put(BytesRef key, BytesRef value) throws IOException {
    ensureOpen();
    if (value == null) {
      delete(key);
      return;
    }
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = getPutOperation(OperationType.PUT, key, value);
      Path path = write(op);
      BytesRef deepCopyOf = BytesRef.deepCopyOf(value);
      _size.addAndGet(deepCopyOf.bytes.length);
      Value old = _pointers.put(BytesRef.deepCopyOf(key), new Value(deepCopyOf, path));
      if (old != null) {
        _size.addAndGet(-old._bytesRef.bytes.length);
      }
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  private void ensureOpenForWriting() throws IOException {
    if (_output == null) {
      openWriter();
    }
  }

  private Path write(Operation op) throws IOException {
    op.write(_output);
    Path p = _outputPath;
    if (_output.getPos() >= _maxAmountAllowedPerFile) {
      rollFile();
    }
    return p;
  }

  private void rollFile() throws IOException {
    LOG.info("Rolling file [" + _outputPath + "]");
    _output.close();
    _output = null;
    openWriter();
  }

  public void cleanupOldFiles() throws IOException {
    _writeLock.lock();
    try {
      if (!isOpenForWriting()) {
        return;
      }
      SortedSet<FileStatus> fileStatusSet = getSortedSet(_path);
      if (fileStatusSet == null || fileStatusSet.size() < 1) {
        return;
      }
      Path newestGen = fileStatusSet.last().getPath();
      if (!newestGen.equals(_outputPath)) {
        throw new IOException("No longer the owner of [" + _path + "]");
      }
      Set<Path> existingFiles = new HashSet<Path>();
      for (FileStatus fileStatus : fileStatusSet) {
        existingFiles.add(fileStatus.getPath());
      }
      Set<Entry<BytesRef, Value>> entrySet = _pointers.entrySet();
      existingFiles.remove(_outputPath);
      for (Entry<BytesRef, Value> e : entrySet) {
        Path p = e.getValue()._path;
        existingFiles.remove(p);
      }
      for (Path p : existingFiles) {
        LOG.info("Removing file no longer referenced [{0}]", p);
        _fileSystem.delete(p, false);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void closeLogFileIfIdle() throws IOException {
    _writeLock.lock();
    try {
      if (_output != null && _lastWrite.get() + MAX_OPEN_FOR_WRITING < System.currentTimeMillis()) {
        // Close writer
        LOG.info("Closing KV log due to inactivity [{0}].", _path);
        try {
          _output.close();
        } finally {
          _output = null;
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private boolean isOpenForWriting() {
    return _output != null;
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
      Value internalValue = _pointers.get(key);
      if (internalValue == null) {
        return false;
      }
      value.copyBytes(internalValue._bytesRef);
      return true;
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public void delete(BytesRef key) throws IOException {
    ensureOpen();
    _writeLock.lock();
    ensureOpenForWriting();
    try {
      Operation op = getDeleteOperation(OperationType.DELETE, key);
      write(op);
      Value old = _pointers.remove(key);
      if (old != null) {
        _size.addAndGet(-old._bytesRef.bytes.length);
      }
    } catch (RemoteException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } catch (LeaseExpiredException e) {
      throw new IOException("Another HDFS KeyStore has likely taken ownership of this key value store.", e);
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (!_isClosed) {
      _isClosed = true;
      _idleLogTimerTask.cancel();
      _oldFileCleanerTimerTask.cancel();
      _hdfsKeyValueTimer.purge();
      _writeLock.lock();
      try {
        if (isOpenForWriting()) {
          syncInternal();
          _output.close();
          _output = null;
        }
      } finally {
        _writeLock.unlock();
      }
    }
  }

  private void openWriter() throws IOException {
    long nextSegment = _currentFileCounter.incrementAndGet();
    String name = buffer(nextSegment);
    _outputPath = new Path(_path, name);
    LOG.info("Opening for writing [{0}].", _outputPath);
    _output = _fileSystem.create(_outputPath, false);
    _output.write(MAGIC);
    _output.writeInt(VERSION);
    syncInternal();
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
    long dfsLength = getDFSLength(inputStream);
    return Math.max(dfsLength, fileStatus.getLen());
  }

  private void syncInternal() throws IOException {
    _output.flush();
    _output.sync();
    _lastWrite.set(System.currentTimeMillis());
    // System.out.println("Sync Output Path [" + _outputPath + "] Position [" +
    // _output.getPos() + "]");
  }

  private void loadIndex(Path path) throws IOException {
    FSDataInputStream inputStream = _fileSystem.open(path);
    byte[] buf = new byte[MAGIC.length];
    inputStream.readFully(buf);
    if (!Arrays.equals(MAGIC, buf)) {
      throw new IOException("File [" + path + "] not a " + BLUR_KEY_VALUE + " file.");
    }
    int version = inputStream.readInt();
    if (version == 1) {
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
          loadIndex(path, operation);
        }
      } finally {
        inputStream.close();
      }
    } else {
      throw new IOException("Unknown version [" + version + "]");
    }
  }

  private void loadIndex(Path path, Operation operation) {
    Value old;
    switch (operation.type) {
    case PUT:
      BytesRef deepCopyOf = BytesRef.deepCopyOf(getKey(operation.value));
      _size.addAndGet(deepCopyOf.bytes.length);
      old = _pointers.put(BytesRef.deepCopyOf(getKey(operation.key)), new Value(deepCopyOf, path));
      break;
    case DELETE:
      old = _pointers.remove(getKey(operation.key));
      break;
    default:
      throw new RuntimeException("Not supported [" + operation.type + "]");
    }
    if (old != null) {
      _size.addAndGet(-old._bytesRef.bytes.length);
    }
  }

  private BytesRef getKey(BytesWritable key) {
    return new BytesRef(key.getBytes(), 0, key.getLength());
  }

  private SortedSet<FileStatus> getSortedSet(Path p) throws IOException {
    if (_fileSystem.exists(p)) {
      FileStatus[] listStatus = _fileSystem.listStatus(p);
      if (listStatus != null) {
        return new TreeSet<FileStatus>(Arrays.asList(listStatus));
      }
    }
    return new TreeSet<FileStatus>();
  }

  private long getDFSLength(FSDataInputStream inputStream) throws IOException {
    try {
      Field field = FilterInputStream.class.getDeclaredField(IN);
      field.setAccessible(true);
      Object dfs = field.get(inputStream);
      Method method = dfs.getClass().getMethod(GET_FILE_LENGTH, new Class[] {});
      Object length = method.invoke(dfs, new Object[] {});
      return (Long) length;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
