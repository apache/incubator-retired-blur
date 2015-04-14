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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;

public class BlurFieldCollector extends Collector implements TopDocCollectorInterface, CloneableCollector {

  public static BlurFieldCollector create(Sort sort, int numHitsToCollect, FieldDoc after, boolean runSlow,
      AtomicBoolean running) {
    return new BlurFieldCollector(sort, numHitsToCollect, after, runSlow, running);
  }

  private int _numHitsToCollect;
  private FieldDoc _after;
  private boolean _runSlow;
  private AtomicBoolean _running;
  private int _hits;
  private TopDocs _topDocs;
  private Sort _sort;

  public BlurFieldCollector(Sort sort, int numHitsToCollect, FieldDoc after, boolean runSlow, AtomicBoolean running) {
    _sort = sort;
    _numHitsToCollect = numHitsToCollect;
    _after = after;
    _runSlow = runSlow;
    _running = running;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void collect(int doc) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public int getTotalHits() {
    return _hits;
  }

  @Override
  public TopDocs topDocs() {
    return _topDocs;
  }

  @Override
  public Collector newCollector() throws IOException {
    TopFieldCollector collector = TopFieldCollector.create(_sort, _numHitsToCollect, _after, true, true, false, true);
    Collector col = new StopExecutionCollector(collector, _running);
    if (_runSlow) {
      return new SlowCollector(col);
    }
    return col;
  }

  @Override
  public Collector merge(Collector... merge) throws IOException {
    List<TopFieldCollector> list = new ArrayList<TopFieldCollector>();
    for (Collector collector : merge) {
      list.add(getTopFieldCollector(collector));
    }
    int i = 0;
    TopDocs[] results = new TopDocs[list.size()];
    for (TopFieldCollector tsdc : list) {
      _hits += tsdc.getTotalHits();
      results[i++] = tsdc.topDocs();
    }
    _topDocs = TopDocs.merge(_sort, _numHitsToCollect, results);
    return this;
  }

  private TopFieldCollector getTopFieldCollector(Collector collector) {
    if (collector instanceof SlowCollector) {
      SlowCollector slowCollector = (SlowCollector) collector;
      return getTopFieldCollector(slowCollector.getCollector());
    } else if (collector instanceof StopExecutionCollector) {
      StopExecutionCollector stopExecutionCollector = (StopExecutionCollector) collector;
      return getTopFieldCollector(stopExecutionCollector.getCollector());
    } else if (collector instanceof TopFieldCollector) {
      TopFieldCollector topFieldCollector = (TopFieldCollector) collector;
      return topFieldCollector;
    } else {
      throw new RuntimeException("Collector type [" + collector + "] not supported.");
    }
  }

}
