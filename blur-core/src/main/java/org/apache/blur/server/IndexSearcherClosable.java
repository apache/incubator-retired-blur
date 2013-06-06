package org.apache.blur.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;
import org.apache.lucene.store.Directory;

public class IndexSearcherClosable extends IndexSearcher implements Closeable {

  private final AtomicReference<NRTManager> _nrtManagerRef;
  private final Directory _directory;

  public IndexSearcherClosable(IndexReader r, ExecutorService executor, AtomicReference<NRTManager> nrtManagerRef,
      Directory directory) {
    super(r, executor);
    _nrtManagerRef = nrtManagerRef;
    _directory = directory;
  }

  public Directory getDirectory() {
    return _directory;
  }

  @Override
  public void close() throws IOException {
    _nrtManagerRef.get().release(this);
  }

}
