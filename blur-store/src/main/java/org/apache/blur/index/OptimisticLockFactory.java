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
import java.util.Arrays;

import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class OptimisticLockFactory extends LockFactory {

  private final Configuration _configuration;
  private final FileSystem _fileSystem;
  private final String _baseLockKey;
  private byte[] _lockKey;
  private final Path _dir;

  public OptimisticLockFactory(Configuration configuration, Path dir, String host, int pid) throws IOException {
    _configuration = configuration;
    _dir = dir;
    _fileSystem = _dir.getFileSystem(_configuration);
    _baseLockKey = host + "/" + pid;
  }

  @Override
  public Lock makeLock(String lockName) {
    final Path lockPath = new Path(_dir, lockName);
    return new Lock() {
      private boolean _set;

      @Override
      public boolean obtain() throws IOException {
        Tracer trace = Trace.trace("filesystem - release", Trace.param("lockPath", lockPath));
        try {
          if (_set) {
            throw new IOException("Lock for [" + _baseLockKey + "] can only be set once.");
          }
          try {
            _lockKey = (_baseLockKey + "/" + System.currentTimeMillis()).getBytes();
            FSDataOutputStream outputStream = _fileSystem.create(lockPath, true);
            outputStream.write(_lockKey);
            outputStream.close();
          } finally {
            _set = true;
          }
          return true;
        } finally {
          trace.done();
        }
      }

      @Override
      public void release() throws IOException {
        Tracer trace = Trace.trace("filesystem - release", Trace.param("lockPath", lockPath));
        try {
          _fileSystem.delete(lockPath, false);
        } finally {
          trace.done();
        }
      }

      @Override
      public boolean isLocked() throws IOException {
        Tracer trace = Trace.trace("filesystem - isLocked", Trace.param("lockPath", lockPath));
        try {
          if (!_set) {
            return false;
          }
          if (!_fileSystem.exists(lockPath)) {
            return false;
          }
          FileStatus fileStatus = _fileSystem.getFileStatus(lockPath);
          long len = fileStatus.getLen();
          if (len != _lockKey.length) {
            return false;
          }
          byte[] buf = new byte[_lockKey.length];
          FSDataInputStream inputStream = _fileSystem.open(lockPath);
          inputStream.readFully(buf);
          inputStream.close();
          if (Arrays.equals(_lockKey, buf)) {
            return true;
          }
          return false;
        } finally {
          trace.done();
        }
      }
    };
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    _fileSystem.delete(new Path(_dir, lockName), false);
  }
}
