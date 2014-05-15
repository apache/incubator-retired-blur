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
package org.apache.blur.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;

public class FacetExecutor {

  private static final Log LOG = LogFactory.getLog(FacetExecutor.class);

  static Comparator<Entry<Object, Info>> COMPARATOR = new Comparator<Entry<Object, Info>>() {
    @Override
    public int compare(Entry<Object, Info> o1, Entry<Object, Info> o2) {
      AtomicReader r1 = o1.getValue()._reader;
      AtomicReader r2 = o2.getValue()._reader;
      return r2.maxDoc() - r1.maxDoc();
    }
  };

  static class Finished extends IOException {
    private static final long serialVersionUID = 1L;
  }

  static class SimpleCollector extends Collector {

    int _hits;
    final OpenBitSet _bitSet;
    Scorer _scorer;

    SimpleCollector(OpenBitSet bitSet) {
      _bitSet = bitSet;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (_bitSet.fastGet(doc)) {
        _hits++;
      } else {
        int nextSetBit = _bitSet.nextSetBit(doc);
        if (nextSetBit < 0) {
          throw new Finished();
        } else {
          int advance = _scorer.advance(nextSetBit);
          if (advance == DocIdSetIterator.NO_MORE_DOCS) {
            throw new Finished();
          }
          if (_bitSet.fastGet(advance)) {
            _hits++;
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      _scorer = scorer;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {

    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

  }

  static class SimpleCollectorExitEarly extends SimpleCollector {

    private long _currentCount;
    private final long _min;

    SimpleCollectorExitEarly(OpenBitSet bitSet, long currentCount, long min) {
      super(bitSet);
      _currentCount = currentCount;
      _min = min;
    }

    @Override
    public void collect(int doc) throws IOException {
      super.collect(doc);
      _currentCount++;
      if (_currentCount >= _min) {
        throw new Finished();
      }
    }

  }

  static class Info {
    final OpenBitSet _bitSet;
    final Scorer[] _scorers;
    final AtomicReader _reader;
    final String _readerStr;
    final int _maxDoc;
    final Lock[] _locks;

    @Override
    public String toString() {
      return "Info scorers length [" + _scorers.length + "] reader [" + _reader + "]";
    }

    Info(AtomicReaderContext context, Scorer[] scorers, Lock[] locks) {
      AtomicReader reader = context.reader();
      _bitSet = new OpenBitSet(reader.maxDoc());
      _scorers = scorers;
      _reader = reader;
      _readerStr = _reader.toString();
      _maxDoc = _reader.maxDoc();
      _locks = locks;
    }

    void process(AtomicLongArray counts, long[] minimumsBeforeReturning, AtomicBoolean running) throws IOException {
      if (minimumsBeforeReturning == null) {
        Tracer trace = Trace.trace("processing facet - segment", Trace.param("reader", _readerStr),
            Trace.param("maxDoc", _maxDoc), Trace.param("minimums", "NONE"), Trace.param("scorers", _scorers.length));
        try {
          for (int i = 0; i < _scorers.length && running.get(); i++) {
            SimpleCollector col = new SimpleCollector(_bitSet);
            runFacet(counts, col, i);
          }
        } finally {
          trace.done();
        }
      } else {
        BlockingQueue<Integer> ids = new ArrayBlockingQueue<Integer>(_scorers.length + 1);
        try {
          populate(ids);
          while (!ids.isEmpty() && running.get()) {
            int id = ids.take();
            Lock lock = _locks[id];
            if (lock.tryLock()) {
              try {
                long min = minimumsBeforeReturning[id];
                long currentCount = counts.get(id);
                if (currentCount < min) {
                  LOG.debug("Running facet, current count [{0}] min [{1}]", currentCount, min);
                  SimpleCollectorExitEarly col = new SimpleCollectorExitEarly(_bitSet, currentCount, min);
                  runFacet(counts, col, id);
                }
              } finally {
                lock.unlock();
              }
            } else {
              ids.put(id);
            }
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

    private void populate(BlockingQueue<Integer> ids) throws InterruptedException {
      for (int i = 0; i < _scorers.length; i++) {
        ids.put(i);
      }
    }

    private void runFacet(AtomicLongArray counts, SimpleCollector col, int i) throws IOException {
      Scorer scorer = _scorers[i];
      if (scorer != null) {
        Tracer traceInner = Trace.trace("processing facet - segment - scorer", Trace.param("scorer", scorer),
            Trace.param("scorer.cost", scorer.cost()));
        try {
          // new ExitScorer(scorer).score(col);
          scorer.score(col);
        } catch (Finished e) {
          // Do nothing, exiting early.
        } finally {
          traceInner.done();
        }
        int hits = col._hits;
        LOG.debug("Facet [{0}] result [{1}]", i, hits);
        counts.addAndGet(i, hits);
      }
      col._hits = 0;
    }
  }

  private final Map<Object, Info> _infoMap = new ConcurrentHashMap<Object, FacetExecutor.Info>();
  private final int _length;
  private final AtomicLongArray _counts;
  private final long[] _minimumsBeforeReturning;
  private final Lock[] _locks;
  private final AtomicBoolean _running;
  private boolean _processed;

  public FacetExecutor(int length) {
    this(length, null, new AtomicLongArray(length));
  }

  public FacetExecutor(int length, long[] minimumsBeforeReturning, AtomicLongArray counts) {
    this(length, minimumsBeforeReturning, counts, new AtomicBoolean(true));
  }

  public FacetExecutor(int length, long[] minimumsBeforeReturning) {
    this(length, minimumsBeforeReturning, new AtomicLongArray(length));
  }

  public FacetExecutor(int length, long[] minimumsBeforeReturning, AtomicLongArray counts, AtomicBoolean running) {
    _length = length;
    _counts = counts;
    _minimumsBeforeReturning = minimumsBeforeReturning;
    _locks = new Lock[_length];
    for (int i = 0; i < _length; i++) {
      _locks[i] = new ReentrantReadWriteLock().writeLock();
    }
    _running = running;
  }

  public void addScorers(AtomicReaderContext context, Scorer[] scorers) throws IOException {
    if (scorers.length != _length) {
      throw new IOException("Scorer length is not correct expecting [" + _length + "] actual [" + scorers.length + "]");
    }
    Object key = getKey(context);
    Info info = _infoMap.get(key);
    if (info == null) {
      info = new Info(context, scorers, _locks);
      _infoMap.put(key, info);
    } else {
      AtomicReader reader = context.reader();
      LOG.warn("Info about reader context [{0}] already created, existing Info [{1}] current reader [{2}].", context,
          info, reader);
    }
  }

  public boolean scorersAlreadyAdded(AtomicReaderContext context) {
    Object key = getKey(context);
    return _infoMap.containsKey(key);
  }

  private Object getKey(AtomicReaderContext context) {
    return context.reader().getCoreCacheKey();
  }

  public OpenBitSet getBitSet(AtomicReaderContext context) throws IOException {
    Info info = _infoMap.get(getKey(context));
    if (info == null) {
      throw new IOException("Info object missing.");
    }
    return info._bitSet;
  }

  public int length() {
    return _length;
  }

  public long get(int i) throws IOException {
    if (!_processed) {
      throw new IOException("Has not been processed.");
    }
    return _counts.get(i);
  }

  public void processFacets(ExecutorService executor) throws IOException {
    if (!_processed) {
      Tracer trace = Trace.trace("processing facets");
      try {
        processInternal(executor);
      } finally {
        trace.done();
      }
      _processed = true;
    }
  }

  private void processInternal(ExecutorService executor) throws IOException {
    List<Entry<Object, Info>> entries = new ArrayList<Entry<Object, Info>>(_infoMap.entrySet());
    Collections.sort(entries, COMPARATOR);
    if (executor == null) {
      for (Entry<Object, Info> e : entries) {
        if (_running.get()) {
          e.getValue().process(_counts, _minimumsBeforeReturning, _running);
        }
      }
    } else {
      final AtomicInteger finished = new AtomicInteger();
      for (Entry<Object, Info> e : entries) {
        if (_running.get()) {
          final Entry<Object, Info> entry = e;
          executor.submit(new Runnable() {
            @Override
            public void run() {
              try {
                entry.getValue().process(_counts, _minimumsBeforeReturning, _running);
              } catch (IOException e) {
                LOG.error("Unknown error", e);
                throw new RuntimeException(e);
              } finally {
                finished.incrementAndGet();
              }
            }
          });
        }
      }

      while (finished.get() < entries.size()) {
        synchronized (this) {
          try {
            wait(1);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }
    }
  }
}
