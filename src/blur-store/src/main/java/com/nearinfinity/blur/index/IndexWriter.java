package com.nearinfinity.blur.index;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

public class IndexWriter extends org.apache.lucene.index.IndexWriter {

  private Lock internalLock;

  public IndexWriter(Directory d, IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException, IOException {
    super(d, conf);
    try {
      internalLock = getInternalLock();
    } catch (Exception e) {
      throw new RuntimeException("Could not get the write lock instance.",e);
    }
  }

  private Lock getInternalLock() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    Field field = org.apache.lucene.index.IndexWriter.class.getDeclaredField("writeLock");
    field.setAccessible(true);
    return (Lock) field.get(this);
  }

  @Override
  protected void doAfterFlush() throws IOException {
    super.doAfterFlush();
    if (!internalLock.isLocked()) {
      throw new IOException("Lock [" + internalLock +"] no longer has write lock.");
    }
  }

  @Override
  protected void doBeforeFlush() throws IOException {
    super.doBeforeFlush();
    if (!internalLock.isLocked()) {
      throw new IOException("Lock [" + internalLock +"] no longer has write lock.");
    }
  }

}
