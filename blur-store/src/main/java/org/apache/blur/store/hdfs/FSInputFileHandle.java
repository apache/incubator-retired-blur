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
package org.apache.blur.store.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.memory.MemoryLeakDetector;
import org.apache.blur.store.hdfs_v2.HdfsUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSInputFileHandle implements Closeable {

  private static final Log LOG = LogFactory.getLog(FSInputFileHandle.class);

  private final FileSystem _fileSystem;
  private final Path _path;
  private final Map<String, ManagedFSDataInputSequentialAccess> _seqAccessInputs;
  private final AtomicBoolean _open = new AtomicBoolean(true);
  private final ManagedFSDataInputRandomAccess _randomAccess;
  private final boolean _resourceTracking;
  private final String _name;

  public FSInputFileHandle(FileSystem fileSystem, Path path, long expectedLength, String name,
      boolean resourceTracking, boolean checkFileLength) throws IOException {
    _resourceTracking = resourceTracking;
    _fileSystem = fileSystem;
    _path = path;
    _name = name;
    _seqAccessInputs = new ConcurrentHashMap<String, ManagedFSDataInputSequentialAccess>();
    FSDataInputStream inputStream;
    if (checkFileLength) {
      final long start = System.nanoTime();
      while (true) {
        inputStream = _fileSystem.open(_path);
        long actualLength = getFileLength(inputStream);
        if (actualLength < expectedLength) {
          inputStream.close();
          if (start + TimeUnit.SECONDS.toNanos(30) < System.nanoTime()) {
            throw new IOException("File path [" + path
                + "] has taken too long to update file length in namenode, expected [" + expectedLength + "] actual ["
                + actualLength + "].");
          }
          LOG.info("Input stream length is incorrect for file [{0}], expected [{1}] actual [{2}]", path,
              expectedLength, actualLength);
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        } else if (actualLength == expectedLength) {
          // We are good !
          break;
        } else {
          throw new IOException("File length expected [" + expectedLength + "] is less than the actual file length ["
              + actualLength + "] for file [" + path + "]");
        }
      }
    } else {
      inputStream = _fileSystem.open(_path);
    }
    _randomAccess = new ManagedFSDataInputRandomAccess(inputStream, _path, expectedLength);
    trackObject(inputStream, "Random Inputstream", name, path);
  }

  private long getFileLength(FSDataInputStream inputStream) throws IOException {
    return HdfsUtils.getDFSLength(inputStream);
  }

  public FSDataInputSequentialAccess openForSequentialInput() throws IOException {
    ensureOpen();
    FSDataInputStream inputStream = _fileSystem.open(_path);
    ManagedFSDataInputSequentialAccess in = new ManagedFSDataInputSequentialAccess(_path, inputStream);
    trackObject(inputStream, "Sequential Inputstream", _name, _path);
    _seqAccessInputs.put(in.getId(), in);
    return in;
  }

  private void ensureOpen() throws IOException {
    if (!_open.get()) {
      throw new IOException("Already closed!");
    }
  }

  public void sequentialInputReset(FSDataInputSequentialAccess sequentialInput) throws IOException {
    ensureOpen();
    ManagedFSDataInputSequentialAccess input = (ManagedFSDataInputSequentialAccess) sequentialInput;
    _seqAccessInputs.remove(input._id);
    sequentialInput.close();
  }

  public FSDataInputRandomAccess getRandomAccess() {
    return _randomAccess;
  }

  @Override
  public void close() throws IOException {
    if (_open.get()) {
      _open.set(true);
      IOUtils.closeQuietly(_randomAccess);
      for (ManagedFSDataInputSequentialAccess in : _seqAccessInputs.values()) {
        in.close();
        IOUtils.closeQuietly(in);
      }
      _seqAccessInputs.clear();
    }
  }

  static class ManagedFSDataInputSequentialAccess implements FSDataInputSequentialAccess {

    final Path _path;
    final FSDataInputStream _input;
    final String _id;

    ManagedFSDataInputSequentialAccess(Path path, FSDataInputStream input) {
      _path = path;
      _input = input;
      _id = UUID.randomUUID().toString();
    }

    String getId() {
      return _id;
    }

    @Override
    public void close() throws IOException {
      _input.close();
    }

    @Override
    public void skip(long amount) throws IOException {
      _input.skip(amount);
    }

    @Override
    public void seek(long filePointer) throws IOException {
      _input.seek(filePointer);
    }

    @Override
    public void readFully(byte[] b, int offset, int length) throws IOException {
      _input.readFully(b, offset, length);
    }

    @Override
    public long getPos() throws IOException {
      return _input.getPos();
    }

    @Override
    public String toString() {
      return _path.toString();
    }

  }

  static class ManagedFSDataInputRandomAccess implements FSDataInputRandomAccess {
    private final FSDataInputStream _inputStream;
    private final Path _path;
    private final long _length;

    ManagedFSDataInputRandomAccess(FSDataInputStream inputStream, Path path, long length) {
      _inputStream = inputStream;
      _path = path;
      _length = length;
    }

    @Override
    public void close() throws IOException {
      _inputStream.close();
    }

    @Override
    public int read(long filePointer, byte[] b, int offset, int length) throws IOException {
      return _inputStream.read(filePointer, b, offset, length);
    }

    @Override
    public String toString() {
      return _path.toString();
    }

    @Override
    public Path getPath() {
      return _path;
    }

    @Override
    public long length() {
      return _length;
    }
  }

  protected <T> void trackObject(T t, String message, Object... args) {
    if (_resourceTracking) {
      MemoryLeakDetector.record(t, message, args);
    }
  }
}
