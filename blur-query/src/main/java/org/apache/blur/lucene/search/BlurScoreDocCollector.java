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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;

public class BlurScoreDocCollector extends CloneableCollector implements TopDocCollectorInterface {

  public static BlurScoreDocCollector create(int numHitsToCollect, ScoreDoc after, boolean runSlow,
      AtomicBoolean running) {
    return new BlurScoreDocCollector(numHitsToCollect, after, runSlow, running);
  }

  private ScoreDoc _after;
  private int _numHitsToCollect;
  private AtomicBoolean _running;
  private boolean _runSlow;
  private int _hits;
  private TopDocs _topDocs;

  public BlurScoreDocCollector(int numHitsToCollect, ScoreDoc after, boolean runSlow, AtomicBoolean running) {
    _numHitsToCollect = numHitsToCollect;
    _after = after;
    _runSlow = runSlow;
    _running = running;
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
  public Collector newCollector() {
    TopScoreDocCollector collector = TopScoreDocCollector.create(_numHitsToCollect, _after, true);
    Collector col = new StopExecutionCollector(collector, _running);
    if (_runSlow) {
      return new SlowCollector(col);
    }
    return col;
  }

  @Override
  public Collector merge(Collector... merge) throws IOException {
    List<TopScoreDocCollector> list = new ArrayList<TopScoreDocCollector>();
    for (Collector collector : merge) {
      list.add(getTopScoreDocCollector(collector));
    }
    int i = 0;
    TopDocs[] results = new TopDocs[list.size()];
    for (TopScoreDocCollector tsdc : list) {
      _hits += tsdc.getTotalHits();
      results[i++] = tsdc.topDocs();
    }
    _topDocs = TopDocs.merge(null, _numHitsToCollect, results);
    return this;
  }

  private TopScoreDocCollector getTopScoreDocCollector(Collector collector) {
    if (collector instanceof SlowCollector) {
      SlowCollector slowCollector = (SlowCollector) collector;
      return getTopScoreDocCollector(slowCollector.getCollector());
    } else if (collector instanceof StopExecutionCollector) {
      StopExecutionCollector stopExecutionCollector = (StopExecutionCollector) collector;
      return getTopScoreDocCollector(stopExecutionCollector.getCollector());
    } else if (collector instanceof TopScoreDocCollector) {
      TopScoreDocCollector topScoreDocCollector = (TopScoreDocCollector) collector;
      return topScoreDocCollector;
    } else {
      throw new RuntimeException("Collector type [" + collector + "] not supported.");
    }
  }
}
