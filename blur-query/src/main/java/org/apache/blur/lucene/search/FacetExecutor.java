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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;

public class FacetExecutor {

  static Comparator<Entry<Object, Info>> COMPARATOR = new Comparator<Entry<Object, Info>>() {
    @Override
    public int compare(Entry<Object, Info> o1, Entry<Object, Info> o2) {
      AtomicReader r1 = o1.getValue()._reader;
      AtomicReader r2 = o2.getValue()._reader;
      return r2.maxDoc() - r1.maxDoc();
    }
  };

  static class SimpleCollector extends Collector {

    int _hits;
    final OpenBitSet _bitSet;

    SimpleCollector(OpenBitSet bitSet) {
      _bitSet = bitSet;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (_bitSet.fastGet(doc)) {
        _hits++;
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {

    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

  }

  static class Info {
    final OpenBitSet _bitSet;
    final Scorer[] _scorers;
    final AtomicReader _reader;

    Info(AtomicReaderContext context, Scorer[] scorers) {
      AtomicReader reader = context.reader();
      _bitSet = new OpenBitSet(reader.maxDoc());
      _scorers = scorers;
      _reader = reader;
    }

    void process(AtomicLongArray counts, long[] minimumsBeforeReturning) throws IOException {
      SimpleCollector col = new SimpleCollector(_bitSet);
      if (minimumsBeforeReturning == null) {
        for (int i = 0; i < _scorers.length; i++) {
          Scorer scorer = _scorers[i];
          if (scorer != null) {
            scorer.score(col);
            counts.addAndGet(i, col._hits);  
          }
          col._hits = 0;
        }
      } else {
        for (int i = 0; i < _scorers.length; i++) {
          long min = minimumsBeforeReturning[i];
          long currentCount = counts.get(i);
          if (currentCount < min) {
            Scorer scorer = _scorers[i];
            if (scorer != null) {
              scorer.score(col);
              counts.addAndGet(i, col._hits);  
            }
            counts.addAndGet(i, col._hits);
            col._hits = 0;
          }
        }
      }
    }
  }

  private final Map<Object, Info> _infoMap = new ConcurrentHashMap<Object, FacetExecutor.Info>();
  private final int _length;
  private final AtomicLongArray _counts;
  private final long[] _minimumsBeforeReturning;
  private boolean _processed;

  public FacetExecutor(int length) {
    this(length, null, new AtomicLongArray(length));
  }

  public FacetExecutor(int length, long[] minimumsBeforeReturning, AtomicLongArray counts) {
    _length = length;
    _counts = counts;
    _minimumsBeforeReturning = minimumsBeforeReturning;
  }

  public FacetExecutor(int length, long[] minimumsBeforeReturning) {
    this(length, minimumsBeforeReturning, new AtomicLongArray(length));
  }

  public void addScorers(AtomicReaderContext context, Scorer[] scorers) throws IOException {
    if (scorers.length != _length) {
      throw new IOException("Scorer length is not correct expecting [" + _length + "] actual [" + scorers.length + "]");
    }
    Object key = getKey(context);
    Info info = _infoMap.get(key);
    if (info == null) {
      info = new Info(context, scorers);
      _infoMap.put(key, info);
    } else {
      throw new IOException("Info about reader context [" + context + "] alread created.");
    }
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
      processInternal(executor);
      _processed = true;
    }
  }

  private void processInternal(ExecutorService executor) throws IOException {
    List<Entry<Object, Info>> entries = new ArrayList<Entry<Object, Info>>(_infoMap.entrySet());
    Collections.sort(entries, COMPARATOR);
    if (executor == null) {
      for (Entry<Object, Info> e : entries) {
        e.getValue().process(_counts, _minimumsBeforeReturning);
      }
    } else {
      final AtomicInteger finished = new AtomicInteger();
      for (Entry<Object, Info> e : entries) {
        final Entry<Object, Info> entry = e;
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              entry.getValue().process(_counts, _minimumsBeforeReturning);
            } catch (IOException e) {
              throw new RuntimeException(e);
            } finally {
              finished.incrementAndGet();
            }
          }
        });
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
