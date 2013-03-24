package org.apache.blur.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NRTManager;

public class IndexSearcherClosable extends IndexSearcher implements Closeable {

  private AtomicReference<NRTManager> _nrtManagerRef;

  public IndexSearcherClosable(IndexReader r, ExecutorService executor, AtomicReference<NRTManager> nrtManagerRef) {
    super(r, executor);
    _nrtManagerRef = nrtManagerRef;
  }

  @Override
  public void close() throws IOException {
    _nrtManagerRef.get().release(this);
  }

}
