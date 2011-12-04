package com.nearinfinity.blur.store.blockcache;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.store.BufferStore;
import com.nearinfinity.blur.store.CustomBufferedIndexInput;

public class BlockDirectory extends DirectIODirectory {

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
    public void update(String name, long blockId, byte[] buffer) {

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
  };

  private DirectIODirectory _directory;
  private int _blockSize;
  private String _dirName;
  private Cache _cache;
  private Set<String> _blockCacheFileTypes;

  public BlockDirectory(String dirName, DirectIODirectory directory) throws IOException {
    this(dirName,directory,NO_CACHE);
  }
  
  public BlockDirectory(String dirName, DirectIODirectory directory, Cache cache) throws IOException {
    this(dirName,directory,NO_CACHE,null);
  }

  public BlockDirectory(String dirName, DirectIODirectory directory, Cache cache, Set<String> blockCacheFileTypes) throws IOException {
    _dirName = dirName;
    _directory = directory;
    _blockSize = BLOCK_SIZE;
    _cache = cache;
    if (_blockCacheFileTypes == null || _blockCacheFileTypes.isEmpty()) {
      _blockCacheFileTypes = null;
    } else {
      _blockCacheFileTypes = blockCacheFileTypes;
    }
    setLockFactory(directory.getLockFactory());
  }

  public IndexInput openInput(String name, int bufferSize) throws IOException {
    final IndexInput source = _directory.openInput(name, _blockSize);
    if (_blockCacheFileTypes == null || isCachableFile(name)) {
      return new CachedIndexInput(source, _blockSize, _dirName, name, _cache, bufferSize);
    }
    return source;
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
  public IndexInput openInput(final String name) throws IOException {
    return openInput(name,_blockSize);
  }

  static class CachedIndexInput extends CustomBufferedIndexInput {

    private IndexInput _source;
    private int _blockSize;
    private long _fileLength;
    private String _cacheName;
    private Cache _cache;

    public CachedIndexInput(IndexInput source, int blockSize, String dirName, String name, Cache cache) {
      super(name);
      _source = source;
      _blockSize = blockSize;
      _fileLength = source.length();
      _cacheName = dirName + "/" + name;
      _cache = cache;
    }

    public CachedIndexInput(IndexInput source, int blockSize, String dirName, String name, Cache cache, int bufferSize) {
      super(name,bufferSize);
      _source = source;
      _blockSize = blockSize;
      _fileLength = source.length();
      _cacheName = dirName + "/" + name;
      _cache = cache;
    }

    @Override
    public Object clone() {
      CachedIndexInput clone = (CachedIndexInput) super.clone();
      clone._source = (IndexInput) _source.clone();
      return clone;
    }

    @Override
    public long length() {
      return _source.length();
    }

//    @Override
//    public void close() throws IOException {
//      _source.close();
//    }

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
      _cache.update(_cacheName, blockId, buf);
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
    _directory.close();
  }

  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  public void copy(Directory to, String src, String dest) throws IOException {
    _directory.copy(to, src, dest);
  }

  public LockFactory getLockFactory() {
    return _directory.getLockFactory();
  }

  public String getLockID() {
    return _directory.getLockID();
  }

  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  @SuppressWarnings("deprecation")
  public void sync(String name) throws IOException {
    _directory.sync(name);
  }

  public String toString() {
    return _directory.toString();
  }

  public IndexOutput createOutput(String name) throws IOException {
    return _directory.createOutput(name);
  }

  public void deleteFile(String name) throws IOException {
    _directory.deleteFile(name);
    _cache.delete(name);
  }

  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  public long fileModified(String name) throws IOException {
    return _directory.fileModified(name);
  }

  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  @SuppressWarnings("deprecation")
  public void touchFile(String name) throws IOException {
    _directory.touchFile(name);
  }

  @Override
  public IndexOutput createOutputDirectIO(String name) throws IOException {
    return _directory.createOutputDirectIO(name);
  }

  @Override
  public IndexInput openInputDirectIO(String name) throws IOException {
    return _directory.openInputDirectIO(name);
  }

}
