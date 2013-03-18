package org.apache.blur.store.blockcache;

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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.blur.store.buffer.BufferStore;
import org.apache.blur.store.buffer.ReusedBufferedIndexInput;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class BlockDirectory extends Directory {

  public static final long BLOCK_SHIFT = 13; // 2^13 = 8,192 bytes per block
  public static final long BLOCK_MOD = 0x1FFF;
  public static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

  public static long getBlock(long pos) {
    return pos >>> BLOCK_SHIFT;
  }

  public static long getPosition(long pos) {
    return pos & BLOCK_MOD;
  }

  public static long getRealPosition(long block, long positionInBlock) {
    return (block << BLOCK_SHIFT) + positionInBlock;
  }

  public static Cache NO_CACHE = new Cache() {

    @Override
    public void update(String name, long blockId, int blockOffset, byte[] buffer, int offset, int length) {
    }

    @Override
    public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
      return false;
    }

    @Override
    public void delete(String name) {

    }

    @Override
    public long size() {
      return 0;
    }

    @Override
    public void renameCacheFile(String source, String dest) {
    }
  };

  private Directory _directory;
  private int _blockSize;
  private String _dirName;
  private Cache _cache;
  private Set<String> _blockCacheFileTypes;

  public BlockDirectory(String dirName, Directory directory) throws IOException {
    this(dirName, directory, NO_CACHE);
  }

  public BlockDirectory(String dirName, Directory directory, Cache cache) throws IOException {
    this(dirName, directory, cache, null);
  }

  public BlockDirectory(String dirName, Directory directory, Cache cache, Set<String> blockCacheFileTypes) throws IOException {
    _dirName = dirName;
    _directory = directory;
    _blockSize = BLOCK_SIZE;
    _cache = cache;
    if (blockCacheFileTypes == null || blockCacheFileTypes.isEmpty()) {
      _blockCacheFileTypes = null;
    } else {
      _blockCacheFileTypes = blockCacheFileTypes;
    }
    setLockFactory(directory.getLockFactory());
  }

  private boolean isCachableFile(String name) {
    for (String ext : _blockCacheFileTypes) {
      if (name.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public IndexInput openInput(final String name, IOContext context) throws IOException {
    final IndexInput source = _directory.openInput(name, context);
    if (_blockCacheFileTypes == null || isCachableFile(name)) {
      return new CachedIndexInput(source, _blockSize, name, getFileCacheName(name), _cache, context);
    }
    return source;
  }

  static class CachedIndexInput extends ReusedBufferedIndexInput {

    private IndexInput _source;
    private int _blockSize;
    private long _fileLength;
    private String _cacheName;
    private Cache _cache;

    public CachedIndexInput(IndexInput source, int blockSize, String name, String cacheName, Cache cache, IOContext context) {
      super(name, context);
      _source = source;
      _blockSize = blockSize;
      _fileLength = source.length();
      _cacheName = cacheName;
      _cache = cache;
    }

    @Override
    public CachedIndexInput clone() {
      CachedIndexInput clone = (CachedIndexInput) super.clone();
      clone._source = (IndexInput) _source.clone();
      return clone;
    }

    @Override
    public long length() {
      return _source.length();
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
    }

    @Override
    protected void readInternal(byte[] b, int off, int len) throws IOException {
      long position = getFilePointer();
      while (len > 0) {
        int length = fetchBlock(position, b, off, len);
        position += length;
        len -= length;
        off += length;
      }
    }

    private int fetchBlock(long position, byte[] b, int off, int len) throws IOException {
      // read whole block into cache and then provide needed data
      long blockId = getBlock(position);
      int blockOffset = (int) getPosition(position);
      int lengthToReadInBlock = Math.min(len, _blockSize - blockOffset);
      if (checkCache(blockId, blockOffset, b, off, lengthToReadInBlock)) {
        return lengthToReadInBlock;
      } else {
        readIntoCacheAndResult(blockId, blockOffset, b, off, lengthToReadInBlock);
      }
      return lengthToReadInBlock;
    }

    private void readIntoCacheAndResult(long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) throws IOException {
      long position = getRealPosition(blockId, 0);
      int length = (int) Math.min(_blockSize, _fileLength - position);
      _source.seek(position);

      byte[] buf = BufferStore.takeBuffer(_blockSize);
      _source.readBytes(buf, 0, length);
      System.arraycopy(buf, blockOffset, b, off, lengthToReadInBlock);
      _cache.update(_cacheName, blockId, 0, buf, 0, _blockSize);
      BufferStore.putBuffer(buf);
    }

    private boolean checkCache(long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
      return _cache.fetch(_cacheName, blockId, blockOffset, b, off, lengthToReadInBlock);
    }

    @Override
    protected void closeInternal() throws IOException {
      _source.close();
    }
  }

  @Override
  public void close() throws IOException {
    String[] files = listAll();
    for (String file : files) {
      _cache.delete(getFileCacheName(file));
    }
    _directory.close();
  }

  String getFileCacheLocation(String name) {
    return _dirName + "/" + name;
  }

  String getFileCacheName(String name) throws IOException {
    return getFileCacheLocation(name) + ":" + getFileModified(name);
  }

  private long getFileModified(String name) throws IOException {
    if (_directory instanceof FSDirectory) {
      File directory = ((FSDirectory) _directory).getDirectory();
      File file = new File(directory,name);
      if (!file.exists()) {
        throw new FileNotFoundException("File [" + name + "] not found");
      }
      return file.lastModified();
    } else if (_directory instanceof HdfsDirectory) {
      return ((HdfsDirectory) _directory).getFileModified(name);
    } else {
      throw new RuntimeException("Not supported");
    }
  }

  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    _directory.copy(to, src, dest, context);
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
  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  public String toString() {
    return _directory.toString();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    IndexOutput dest = _directory.createOutput(name, context);
//    if (_blockCacheFileTypes == null || isCachableFile(name)) {
//      return new CachedIndexOutput(this, dest, _blockSize, name, _cache, _blockSize);
//    }
    return dest;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    _cache.delete(getFileCacheName(name));
    _directory.deleteFile(name);
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
  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  public Directory getDirectory() {
    return _directory;
  }

}
