package com.nearinfinity.blur.store.lock;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class BlurLockFactory extends LockFactory {

  private final Configuration _configuration;
  private final FileSystem _fileSystem;
  private final String _baseLockKey;
  private byte[] _lockKey;
  private final Path _dir;

  public BlurLockFactory(Configuration configuration, Path dir, String host, int pid) throws IOException {
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
      }

      @Override
      public void release() throws IOException {
        _fileSystem.delete(lockPath, false);
      }

      @Override
      public boolean isLocked() throws IOException {
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
      }
    };
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    _fileSystem.delete(new Path(_dir, lockName), false);
  }
}
