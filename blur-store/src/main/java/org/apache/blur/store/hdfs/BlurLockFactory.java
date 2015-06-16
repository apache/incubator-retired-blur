package org.apache.blur.store.hdfs;

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
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class BlurLockFactory extends LockFactory {

  private static final Log LOG = LogFactory.getLog(BlurLockFactory.class);

  private final Configuration _configuration;
  private final String _baseLockKey;
  private final Path _dir;
  private final FileSystem _fileSystem;

  public BlurLockFactory(Configuration configuration, Path dir, String host, String pid) throws IOException {
    _configuration = configuration;
    _dir = dir;
    _baseLockKey = host + "/" + pid;
    _fileSystem = _dir.getFileSystem(_configuration);
  }

  static class BlurLock extends Lock {

    private static final String UTF_8 = "UTF-8";
    private final String _lockName;
    private final Path _dir;
    private final String _baseLockKey;
    private final FileSystem _fileSystem;
    private boolean _set;
    private long _id;

    BlurLock(FileSystem fileSystem, Path dir, String lockName, String baseLockKey) {
      _fileSystem = fileSystem;
      _dir = dir;
      _lockName = lockName;
      _baseLockKey = baseLockKey;
    }

    @Override
    public boolean obtain() throws IOException {
      Tracer trace = Trace.trace("filesystem - obtain", Trace.param("dir", _dir), Trace.param("lockName", _lockName));
      try {
        if (_set) {
          throw new IOException("Lock for [" + _baseLockKey + "] can only be set once.");
        }
        try {
          long id = findNextLockId();
          try {
            LOG.info("Writing lock [{0}] with id [{1}] for [{2}]", _lockName, id, _dir);
            FSDataOutputStream outputStream = _fileSystem.create(getLockPath(id), false);
            outputStream.write(_baseLockKey.getBytes(UTF_8));
            outputStream.close();
            _id = id;
            cleanupOldLocks();
            return true;
          } catch (IOException e) {
            return false;
          }
        } finally {
          _set = true;
        }
      } finally {
        trace.done();
      }
    }

    private void cleanupOldLocks() throws IOException {
      FileStatus[] fileStatusForLock = getFileStatusForLock(_fileSystem, _dir, _lockName);
      for (FileStatus fileStatus : fileStatusForLock) {
        long id = getId(fileStatus);
        if (id < _id) {
          LOG.info("Deleting old lock [{0}] with id [{1}] for [{2}]", _lockName, id, _dir);
          _fileSystem.delete(getLockPath(id), false);
        }
      }
    }

    private Path getLockPath(long id) {
      return new Path(_dir, _lockName + "." + id);
    }

    private long findNextLockId() throws IOException {
      FileStatus[] listStatus = getFileStatusForLock(_fileSystem, _dir, _lockName);
      if (listStatus == null || listStatus.length == 0) {
        return 0L;
      }
      long largestId = -1L;
      for (FileStatus fileStatus : listStatus) {
        long id = getId(fileStatus);
        if (id > largestId) {
          largestId = id;
        }
      }
      return largestId + 1L;
    }

    private long getId(FileStatus fileStatus) {
      String name = fileStatus.getPath().getName();
      int lastIndexOf = name.lastIndexOf('.');
      long id = Long.parseLong(name.substring(lastIndexOf + 1));
      return id;
    }

    @Override
    public boolean isLocked() throws IOException {
      Tracer trace = Trace.trace("filesystem - isLocked", Trace.param("dir", _dir), Trace.param("lockName", _lockName));
      try {
        // test my file
        Path myLockPath = getLockPath(_id);
        Path nextLockPath = getLockPath(_id + 1);
        boolean e1 = _fileSystem.exists(myLockPath);
        if (!e1) {
          LOG.info("Lock lost [{0}] with id [{1}] for [{2}]", _lockName, _id, _dir);
          return false;
        }
        // test next file
        boolean e2 = _fileSystem.exists(nextLockPath);
        if (e2) {
          LOG.info("Lock lost [{0}] with id [{1}] for [{2}]", _lockName, _id, _dir);
          return false;
        }
        return true;
      } finally {
        trace.done();
      }
    }

    @Override
    public void release() throws IOException {
      Tracer trace = Trace.trace("filesystem - release", Trace.param("dir", _dir), Trace.param("lockName", _lockName));
      try {
        LOG.info("Releasing Lock [{0}] with id [{1}] for [{2}]", _lockName, _id, _dir);
        _fileSystem.delete(getLockPath(_id), false);
      } finally {
        trace.done();
      }
    }

  }

  @Override
  public Lock makeLock(String lockName) {
    return new BlurLock(_fileSystem, _dir, lockName, _baseLockKey);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    LOG.info("Clearing lock [{0}] with for [{2}]", lockName, _dir);
    FileStatus[] fileStatusForLock = getFileStatusForLock(_fileSystem, _dir, lockName);
    for (FileStatus fileStatus : fileStatusForLock) {
      _fileSystem.delete(fileStatus.getPath(), false);
    }
  }

  private static FileStatus[] getFileStatusForLock(FileSystem fileSystem, Path dir, final String lockName)
      throws FileNotFoundException, IOException {
    FileStatus[] listStatus = fileSystem.listStatus(dir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(lockName + ".");
      }
    });
    return listStatus;
  }
}