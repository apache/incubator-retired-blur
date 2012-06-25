package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class DirectoryReferenceFileGC extends TimerTask {

  private static final Log LOG = LogFactory.getLog(DirectoryReferenceFileGC.class);

  private Timer _timer;
  private long _delay = 5000;
  private LinkedBlockingQueue<Value> _queue;

  public static class Value {
    public Value(Directory directory, String name, Map<String, AtomicInteger> refs) {
      this.directory = directory;
      this.name = name;
      this.refs = refs;
    }

    Directory directory;
    String name;
    Map<String, AtomicInteger> refs;

    public boolean tryToDelete() throws IOException {
      AtomicInteger counter = refs.get(name);
      if (counter.get() <= 0) {
        refs.remove(name);
        LOG.debug("Removing file [{0}]", name);
        directory.deleteFile(name);
        return true;
      } else {
        LOG.debug("File [{0}] had too many refs [{1}]", name, counter.get());
      }
      return false;
    }
  }

  public void init() {
    _timer = new Timer("Blur-File-GC", true);
    _timer.scheduleAtFixedRate(this, _delay, _delay);
    _queue = new LinkedBlockingQueue<Value>();
  }

  public void add(Directory directory, String name, Map<String, AtomicInteger> refs) {
    try {
      _queue.put(new Value(directory, name, refs));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    _timer.purge();
    _timer.cancel();
  }

  @Override
  public void run() {
    Iterator<Value> iterator = _queue.iterator();
    while (iterator.hasNext()) {
      Value value = iterator.next();
      try {
        if (value.tryToDelete()) {
          iterator.remove();
        }
      } catch (IOException e) {
        LOG.error("Unknown error", e);
      }
    }
  }
}
