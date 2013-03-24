package org.apache.blur.manager.indexserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class CloseableExecutorService implements Closeable {

  private ExecutorService executor;

  public CloseableExecutorService(ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public void close() throws IOException {

  }

}
