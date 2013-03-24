package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;

public class SharedMergeScheduler extends MergeScheduler implements Runnable {

  private static final Log LOG = LogFactory.getLog(SharedMergeScheduler.class);

  private static final long ONE_SECOND = 1000;

  private BlockingQueue<IndexWriter> _writers = new LinkedBlockingQueue<IndexWriter>();
  private AtomicBoolean _running = new AtomicBoolean(true);
  private ExecutorService service;

  public SharedMergeScheduler() {
    int threads = 3;
    service = Executors.newThreadPool("sharedMergeScheduler", threads, false);
    for (int i = 0; i < threads; i++) {
      service.submit(this);
    }
  }

  @Override
  public void merge(IndexWriter writer) throws IOException {
    synchronized (_writers) {
      if (!_writers.contains(writer)) {
        LOG.debug("Adding writer to merge [{0}]", writer);
        _writers.add(writer);
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    service.shutdownNow();
  }

  @Override
  public void run() {
    while (_running.get()) {
      try {
        IndexWriter writer;
        synchronized (_writers) {
          writer = _writers.poll();
        }
        if (writer == null) {
          synchronized (this) {
            wait(ONE_SECOND);
          }
        } else if (mergeWriter(writer)) {
          // there seems to be more merges to do
          merge(writer);
        }
      } catch (InterruptedException e) {
        LOG.info("Merging interrupted, exiting.");
        return;
      } catch (IOException e) {
        LOG.error("Unknown IOException", e);
      }
    }
  }

  private boolean mergeWriter(IndexWriter writer) throws IOException {
    MergePolicy.OneMerge merge = writer.getNextMerge();
    if (merge == null) {
      LOG.debug("No merges to run for [{0}]", writer);
      return false;
    }
    long s = System.currentTimeMillis();
    writer.merge(merge);
    long e = System.currentTimeMillis();
    double time = (e - s) / 1000.0;
    double rate = (merge.totalBytesSize() / 1024 / 1024) / time;
    LOG.info("Merge took [{0} s] to complete at rate of [{1} MB/s]", time, rate);
    return true;
  }

}
