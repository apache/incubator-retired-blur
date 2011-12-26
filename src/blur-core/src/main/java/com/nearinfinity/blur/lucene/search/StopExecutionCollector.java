package com.nearinfinity.blur.lucene.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public class StopExecutionCollector extends Collector {
  
  private static final long _5MS = TimeUnit.MILLISECONDS.toNanos(5);
  
  private Collector _collector;
  private AtomicBoolean _running;
  private long last;
  
  public StopExecutionCollector(Collector collector, AtomicBoolean running) {
    _collector = collector;
    _running = running;
  }
  
  public static class StopExecutionCollectorException extends RuntimeException {
    private static final long serialVersionUID = 5753875017543945163L;
  }

  public boolean acceptsDocsOutOfOrder() {
    return _collector.acceptsDocsOutOfOrder();
  }

  public void collect(int doc) throws IOException {
    long now = System.nanoTime();
    if (last + _5MS < now) {
      if (!_running.get()) {
        throw new StopExecutionCollectorException();
      }
      last = now;
    }
    _collector.collect(doc);
  }

  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    _collector.setNextReader(reader, docBase);
  }

  public void setScorer(Scorer scorer) throws IOException {
    _collector.setScorer(scorer);
  }

}
