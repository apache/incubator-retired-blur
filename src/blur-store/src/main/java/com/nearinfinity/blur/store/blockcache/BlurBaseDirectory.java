package com.nearinfinity.blur.store.blockcache;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class BlurBaseDirectory extends Directory {
  
  public static final long BLOCK_SHIFT = 14; // 2^14 = 16,384 bytes per block
  public static final long BLOCK_MOD = 0x3FFF;
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

  private Directory _directory;
  private int _blockSize;
  private DirectoryCache _cache = new DirectoryCache() {
    
    @Override
    public void update(String name, long blockId, byte[] buffer) {
      
    }
    
    @Override
    public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
      return false;
    }
  };
  
  public BlurBaseDirectory(Directory directory) throws IOException {
    _directory = directory;
    _blockSize = BLOCK_SIZE;
    setLockFactory(directory.getLockFactory());
  }

  public BlurBaseDirectory(Directory directory, DirectoryCache cache) throws IOException {
    _directory = directory;
    _blockSize = BLOCK_SIZE;
    _cache = cache;
    setLockFactory(directory.getLockFactory());
  }
  
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    final IndexInput source = _directory.openInput(name, _blockSize);
    return new CachedIndexInput(source, _blockSize, name, _cache, bufferSize);
  }
  
  @Override
  public IndexInput openInput(final String name) throws IOException {
    final IndexInput source = _directory.openInput(name, _blockSize);
    return new CachedIndexInput(source, _blockSize, name, _cache);
  }

  static class CachedIndexInput extends BufferedIndexInput {

    private IndexInput _source;
    private int _blockSize;
    private long _fileLength;
    private byte[] _buffer;
    private String _name;
    private DirectoryCache _cache;

    public CachedIndexInput(IndexInput source, int blockSize, String name, DirectoryCache cache) {
      _source = source;
      _blockSize = blockSize;
      _fileLength = source.length();
      _name = name;
      _cache = cache;
      _buffer = new byte[_blockSize];
    }
    
    public CachedIndexInput(IndexInput source, int blockSize, String name, DirectoryCache cache, int bufferSize) {
      super(bufferSize);
      _source = source;
      _blockSize = blockSize;
      _fileLength = source.length();
      _name = name;
      _cache = cache;
      _buffer = new byte[_blockSize];
    }

    @Override
    public Object clone() {
      CachedIndexInput clone = (CachedIndexInput) super.clone();
      clone._source = (IndexInput) _source.clone();
      clone._buffer = new byte[_blockSize];
      return clone;
    }

    @Override
    public long length() {
      return _source.length();
    }

    @Override
    public void close() throws IOException {
      _source.close();
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
      long blockId = getBlock(position);
      int blockOffset = (int) getPosition(position);
      int lengthToReadInBlock = Math.min(len, _blockSize - blockOffset);
      if (checkCache(blockId, blockOffset, lengthToReadInBlock, b, off)) {
        return lengthToReadInBlock;
      } else {
        readIntoCacheAndResult(blockId, blockOffset, lengthToReadInBlock, b, off);
      }
      return lengthToReadInBlock;
    }

    private void readIntoCacheAndResult(long blockId, int blockOffset, int lengthToReadInBlock, byte[] b, int off) throws IOException {
      long position = getRealPosition(blockId,0);
      int length = (int) Math.min(_blockSize, _fileLength - position);
      _source.seek(position);
      _source.readBytes(_buffer, 0, length);
      System.arraycopy(_buffer, blockOffset, b, off, lengthToReadInBlock);
      _cache.update(_name,blockId,_buffer);
    }

    private boolean checkCache(long blockId, int blockOffset, int lengthToReadInBlock, byte[] b, int off) {
      return _cache.fetch(_name,blockId,blockOffset,b,off,lengthToReadInBlock);
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

}
