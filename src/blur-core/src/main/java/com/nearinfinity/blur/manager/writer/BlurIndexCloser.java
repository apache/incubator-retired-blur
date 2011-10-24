package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;

public class BlurIndexCloser implements Runnable {

  private static final Log LOG = LogFactory.getLog(BlurIndexCloser.class);
  private static final long PAUSE_TIME = TimeUnit.SECONDS.toMillis(2);
  private Thread daemon;
  private Collection<IndexReader> readers = new LinkedBlockingQueue<IndexReader>();
  private AtomicBoolean running = new AtomicBoolean();

  public void init() {
    LOG.info("init - start");
    running.set(true);
    daemon = new Thread(this);
    daemon.setDaemon(true);
    daemon.setName(getClass().getName() + "-Daemon");
    daemon.start();
    LOG.info("init - complete");
  }

  public void close() {
    running.set(false);
    daemon.interrupt();
  }

  public void close(IndexReader reader) {
    // @TODO need to a pause to the check for closed because of a race
    // condition.
    readers.add(reader);
  }

  @Override
  public void run() {
    while (running.get()) {
      tryToCloseReaders();
      try {
        Thread.sleep(PAUSE_TIME);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  private void tryToCloseReaders() {
    Iterator<IndexReader> it = readers.iterator();
    while (it.hasNext()) {
      IndexReader reader = it.next();
      if (reader.getRefCount() == 1) {
        try {
          LOG.debug("Closing indexreader [" + reader + "].");
          reader.close();
        } catch (IOException e) {
          LOG.error("Error while trying to close indexreader [" + reader + "].", e);
        }
        it.remove();
      }
    }
  }
}