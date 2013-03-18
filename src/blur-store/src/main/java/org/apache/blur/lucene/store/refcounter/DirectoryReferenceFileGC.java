package org.apache.blur.lucene.store.refcounter;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.store.Directory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class DirectoryReferenceFileGC extends TimerTask {

  private static final String ORG_APACHE_BLUR = "org.apache.blur";

  private static final Log LOG = LogFactory.getLog(DirectoryReferenceFileGC.class);

  private Timer _timer;
  private long _delay = 5000;
  private LinkedBlockingQueue<Value> _queue;
  private volatile int numberOfFilesToBeDeleted = 0;

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
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, "Lucene", "Files in Queue to be Deleted"), new Gauge<Integer>() {
      @Override
      public Integer value() {
        return numberOfFilesToBeDeleted;
      }
    });
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
    int count = 0;
    while (iterator.hasNext()) {
      Value value = iterator.next();
      try {
        if (value.tryToDelete()) {
          iterator.remove();
        } else {
          count++;
        }
      } catch (IOException e) {
        LOG.error("Unknown error", e);
      }
    }
    numberOfFilesToBeDeleted = count;
  }
}
