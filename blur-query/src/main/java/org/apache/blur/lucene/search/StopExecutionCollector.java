package org.apache.blur.lucene.search;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public class StopExecutionCollector extends Collector {

  private static final long _5MS = TimeUnit.MILLISECONDS.toNanos(5);

  private final Collector _collector;
  private final AtomicBoolean _running;
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

  public void setNextReader(AtomicReaderContext context) throws IOException {
    _collector.setNextReader(context);
  }

  public void setScorer(Scorer scorer) throws IOException {
    _collector.setScorer(scorer);
  }

  public Collector getCollector() {
    return _collector;
  }

}
