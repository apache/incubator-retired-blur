package com.nearinfinity.blur.store.blockcache;

public interface DirectoryCache {

  void update(String name, long blockId, byte[] buffer);

  // System.arraycopy(source block here, offset to beginning of block +
  // blockOffset, b, off, lengthToReadInBlock);
  boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock);

}
