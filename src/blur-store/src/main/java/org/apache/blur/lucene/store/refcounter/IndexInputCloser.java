package org.apache.blur.lucene.store.refcounter;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

public class IndexInputCloser implements Runnable {

  private static final Log LOG = LogFactory.getLog(IndexInputCloser.class);

  private ReferenceQueue<IndexInput> referenceQueue = new ReferenceQueue<IndexInput>();
  private Thread daemon;
  private AtomicBoolean running = new AtomicBoolean();
  private Collection<IndexInputCloserRef> refs = new HashSet<IndexInputCloserRef>();

  static class IndexInputCloserRef extends WeakReference<IndexInput> implements Closeable {
    public IndexInputCloserRef(IndexInput referent, ReferenceQueue<? super IndexInput> q) {
      super(referent, q);
    }

    @Override
    public void close() throws IOException {
      IndexInput input = get();
      if (input != null) {
        LOG.debug("Closing indexinput [{0}]", input);
        input.close();
      }
    }
  }

  public void init() {
    running.set(true);
    daemon = new Thread(this);
    daemon.setDaemon(true);
    daemon.setName("IndexIndexCloser");
    daemon.start();
  }

  public void add(IndexInput indexInput) {
    LOG.debug("Adding [{0}]", indexInput);
    IndexInputCloserRef ref = new IndexInputCloserRef(indexInput, referenceQueue);
    synchronized (refs) {
      refs.add(ref);
    }
  }

  public void close() {
    running.set(false);
    refs.clear();
    daemon.interrupt();
  }

  @Override
  public void run() {
    while (running.get()) {
      try {
        IndexInputCloserRef ref = (IndexInputCloserRef) referenceQueue.remove();
        LOG.debug("Closing [{0}]", ref);
        IOUtils.closeWhileHandlingException(ref);
        synchronized (refs) {
          refs.remove(ref);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted");
        running.set(false);
        return;
      }
    }
  }

}
