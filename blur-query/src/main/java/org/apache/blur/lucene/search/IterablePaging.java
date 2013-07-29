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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.index.ExitableReader.ExitingReaderException;
import org.apache.blur.lucene.search.StopExecutionCollector.StopExecutionCollectorException;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.utils.BlurIterable;
import org.apache.blur.utils.BlurIterator;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * The {@link IterablePaging} class allows for easy paging through lucene hits.
 */
public class IterablePaging implements BlurIterable<ScoreDoc, BlurException> {

  private final IndexSearcher searcher;
  private final Query query;
  private final AtomicBoolean running;
  private final int numHitsToCollect;
  private final boolean _runSlow;

  private TotalHitsRef totalHitsRef;
  private ProgressRef progressRef;
  private int skipTo;
  private int gather = -1;

  public IterablePaging(AtomicBoolean running, IndexSearcher searcher, Query query, int numHitsToCollect,
      TotalHitsRef totalHitsRef, ProgressRef progressRef, boolean runSlow) throws BlurException {
    this.running = running;
    try {
      this.query = searcher.rewrite(query);
    } catch (IOException e) {
      throw new BException("Unknown error during rewrite", e);
    }
    this.searcher = searcher;
    this.numHitsToCollect = numHitsToCollect;
    this.totalHitsRef = totalHitsRef == null ? new TotalHitsRef() : totalHitsRef;
    this.progressRef = progressRef == null ? new ProgressRef() : progressRef;
    _runSlow = runSlow;
  }

  public static class TotalHitsRef {
    // This is an atomic integer because more than likely if there is
    // any status sent to the user, it will be done in another thread.
    protected AtomicInteger totalHits = new AtomicInteger(0);

    public int totalHits() {
      return totalHits.get();
    }
  }

  public static class ProgressRef {
    // These are atomic integers because more than likely if there is
    // any status sent to the user, it will be done in another thread.
    protected AtomicInteger skipTo = new AtomicInteger(0);
    protected AtomicInteger currentHitPosition = new AtomicInteger(0);
    protected AtomicInteger searchesPerformed = new AtomicInteger(0);
    protected AtomicLong queryTime = new AtomicLong(0);

    public int skipTo() {
      return skipTo.get();
    }

    public int currentHitPosition() {
      return currentHitPosition.get();
    }

    public int searchesPerformed() {
      return searchesPerformed.get();
    }

    public long queryTime() {
      return queryTime.get();
    }
  }

  /**
   * Gets the total hits of the search.
   * 
   * @return the total hits.
   */
  public int getTotalHits() {
    return totalHitsRef.totalHits();
  }

  /**
   * Allows for gathering of the total hits of this search.
   * 
   * @param ref
   *          {@link TotalHitsRef}.
   * @return this.
   */
  public IterablePaging totalHits(TotalHitsRef ref) {
    totalHitsRef = ref;
    return this;
  }

  /**
   * Skips the first x number of hits.
   * 
   * @param skipTo
   *          the number hits to skip.
   * @return this.
   */
  public IterablePaging skipTo(int skipTo) {
    this.skipTo = skipTo;
    return this;
  }

  /**
   * Only gather up to x number of hits.
   * 
   * @param gather
   *          the number of hits to gather.
   * @return this.
   */
  public IterablePaging gather(int gather) {
    this.gather = gather;
    return this;
  }

  /**
   * Allows for gathering the progress of the paging.
   * 
   * @param ref
   *          the {@link ProgressRef}.
   * @return this.
   */
  public IterablePaging progress(ProgressRef ref) {
    this.progressRef = ref;
    return this;
  }

  /**
   * The {@link ScoreDoc} iterator.
   * 
   * @throws BlurException
   */
  @Override
  public BlurIterator<ScoreDoc, BlurException> iterator() throws BlurException {
    return skipHits(new PagingIterator());
  }

  class PagingIterator implements BlurIterator<ScoreDoc, BlurException> {
    private static final String STOP_EXECUTION_COLLECTOR_EXCEPTION = "StopExecutionCollectorException";
    private ScoreDoc[] scoreDocs;
    private int counter = 0;
    private int offset = 0;
    private int endPosition = gather == -1 ? Integer.MAX_VALUE : skipTo + gather;
    private ScoreDoc lastScoreDoc;

    PagingIterator() throws BlurException {
      search();
    }

    void search() throws BlurException {
      long s = System.currentTimeMillis();
      progressRef.searchesPerformed.incrementAndGet();
      try {
        TopScoreDocCollector collector = TopScoreDocCollector.create(numHitsToCollect, lastScoreDoc, true);
        Collector col = new StopExecutionCollector(collector, running);
        if (_runSlow) {
          col = new SlowCollector(col);
        }
        searcher.search(query, col);
        totalHitsRef.totalHits.set(collector.getTotalHits());
        TopDocs topDocs = collector.topDocs();
        scoreDocs = topDocs.scoreDocs;
      } catch (StopExecutionCollectorException e) {
        throw new BlurException(STOP_EXECUTION_COLLECTOR_EXCEPTION, null, ErrorType.UNKNOWN);
      } catch (ExitingReaderException e) {
        throw new BlurException(STOP_EXECUTION_COLLECTOR_EXCEPTION, null, ErrorType.UNKNOWN);
      } catch (IOException e) {
        throw new BException("Unknown error during search call", e);
      }
      if (scoreDocs.length > 0) {
        lastScoreDoc = scoreDocs[scoreDocs.length - 1];
      } else {
        lastScoreDoc = null;
      }
      long e = System.currentTimeMillis();
      progressRef.queryTime.addAndGet(e - s);
    }

    @Override
    public boolean hasNext() {
      return counter < totalHitsRef.totalHits() && counter < endPosition ? true : false;
    }

    @Override
    public ScoreDoc next() throws BlurException {
      if (isCurrentCollectorExhausted()) {
        search();
        offset = 0;
      }
      progressRef.currentHitPosition.set(counter);
      counter++;
      return scoreDocs[offset++];
    }

    private boolean isCurrentCollectorExhausted() {
      return offset < scoreDocs.length ? false : true;
    }
  }

  private BlurIterator<ScoreDoc, BlurException> skipHits(BlurIterator<ScoreDoc, BlurException> iterator)
      throws BlurException {
    progressRef.skipTo.set(skipTo);
    for (int i = 0; i < skipTo && iterator.hasNext(); i++) {
      // eats the hits, and moves the iterator to the desired skip to position.
      progressRef.currentHitPosition.set(i);
      iterator.next();
    }
    return iterator;
  }

}