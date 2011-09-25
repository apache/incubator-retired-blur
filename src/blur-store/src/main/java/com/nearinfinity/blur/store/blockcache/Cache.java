package com.nearinfinity.blur.store.blockcache;

public interface Cache {

  void delete(String name);
  void update(String name, long blockId, byte[] buffer);
  boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock);

}
