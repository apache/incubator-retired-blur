/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.OpenBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.utils.PrimeDocCache.IndexReaderCache;

@SuppressWarnings("deprecation")
public class SuperQuery extends AbstractWrapperQuery {

  private static final long serialVersionUID = -5901574044714034398L;
  private ScoreType scoreType;

  public SuperQuery(Query query, ScoreType scoreType) {
    super(query, false);
    this.scoreType = scoreType;
  }

  public SuperQuery(Query query, ScoreType scoreType, boolean rewritten) {
    super(query, rewritten);
    this.scoreType = scoreType;
  }

  public Object clone() {
    return new SuperQuery((Query) query.clone(), scoreType, rewritten);
  }

  public Weight createWeight(Searcher searcher) throws IOException {
    if (searcher instanceof BlurSearcher) {
      IndexReaderCache indexReaderCache = ((BlurSearcher) searcher).getIndexReaderCache();
      Weight weight = query.createWeight(searcher);
      return new SuperWeight(weight, query.toString(), this, indexReaderCache, scoreType);
    }
    throw new UnsupportedOperationException("Searcher must be a blur seacher.");
  }

  public Query rewrite(IndexReader reader) throws IOException {
    if (rewritten) {
      return this;
    }
    return new SuperQuery(query.rewrite(reader), scoreType, true);
  }

  public String toString() {
    return "super:{" + query.toString() + "}";
  }

  public String toString(String field) {
    return "super:{" + query.toString(field) + "}";
  }

  public static class SuperWeight extends Weight {

    private static final long serialVersionUID = -4832849792097064960L;

    private Weight weight;
    private String originalQueryStr;
    private Query query;
    private ScoreType scoreType;
    private IndexReaderCache indexReaderCache;

    public SuperWeight(Weight weight, String originalQueryStr, Query query, IndexReaderCache indexReaderCache, ScoreType scoreType) {
      this.weight = weight;
      this.originalQueryStr = originalQueryStr;
      this.query = query;
      this.indexReaderCache = indexReaderCache;
      this.scoreType = scoreType;
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      throw new RuntimeException("not supported");
    }

    @Override
    public Query getQuery() {
      return query;
    }

    @Override
    public float getValue() {
      return weight.getValue();
    }

    @Override
    public void normalize(float norm) {
      weight.normalize(norm);
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      Scorer scorer = weight.scorer(reader, true, topScorer);
      if (scorer == null) {
        return null;
      }
      if (reader instanceof SegmentReader) {
        OpenBitSet primeDocBitSet = indexReaderCache.getPrimeDocBitSet((SegmentReader) reader);
        return new SuperScorer(scorer, primeDocBitSet, originalQueryStr, scoreType);
      } else {
        throw new UnsupportedOperationException("Reader is not a segment reader.");
      }
    }

    @Override
    public float sumOfSquaredWeights() throws IOException {
      return weight.sumOfSquaredWeights();
    }
  }

  @SuppressWarnings("unused")
  public static class SuperScorer extends Scorer {

    private final static Logger LOG = LoggerFactory.getLogger(SuperScorer.class);
    private static final double SUPER_POWER_CONSTANT = 2;
    private Scorer scorer;
    private OpenBitSet bitSet;
    private int nextPrimeDoc;
    private int primeDoc = -1;
    private String originalQueryStr;
    private ScoreType scoreType;

    private int numDocs;
    private float bestScore;
    private float aggregateScore;
    private int hitsInEntity;

    protected SuperScorer(Scorer scorer, OpenBitSet bitSet, String originalQueryStr, ScoreType scoreType) {
      super(scorer.getSimilarity());
      this.scorer = scorer;
      this.bitSet = bitSet;
      this.originalQueryStr = originalQueryStr;
      this.scoreType = scoreType;
    }

    @Override
    public float score() throws IOException {
      switch (scoreType) {
      case AGGREGATE:
        return aggregateScore;
      case BEST:
        return bestScore;
      case CONSTANT:
        return 1;
      case SUPER:
        double log = Math.log10(aggregateScore) + 1.0;
        double avg = aggregateScore / hitsInEntity;
        double pow = Math.pow(avg, SUPER_POWER_CONSTANT);
        return (float) Math.pow(log + pow, 1.0 / SUPER_POWER_CONSTANT);
      }
      throw new RuntimeException("Unknown Score type[" + scoreType + "]");
    }

    @Override
    public int docID() {
      return primeDoc;
    }

    @Override
    public int advance(int target) throws IOException {
      if (target == NO_MORE_DOCS) {
        return scorer.advance(NO_MORE_DOCS);
      }
      int doc = scorer.docID();
      int odoc = doc;
      if (isScorerExhausted(doc)) {
        return primeDoc = doc;
      }
      if (target > doc || doc == -1) {
        doc = scorer.advance(target);
        if (isScorerExhausted(doc)) {
          return primeDoc = doc;
        }
      } else if (isScorerExhausted(doc)) {
        return primeDoc == -1 ? primeDoc = doc : primeDoc;
      }
      int gatherAllHitsSuperDoc = gatherAllHitsSuperDoc(doc);
      return gatherAllHitsSuperDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = scorer.docID();
      int odoc = doc;
      if (isScorerExhausted(doc)) {
        return primeDoc = doc;
      }
      if (doc == -1) {
        doc = scorer.nextDoc();
        if (isScorerExhausted(doc)) {
          return primeDoc = doc;
        }
      } else if (isScorerExhausted(doc)) {
        return primeDoc == -1 ? primeDoc = doc : primeDoc;
      }

      int gatherAllHitsSuperDoc = gatherAllHitsSuperDoc(doc);
      return gatherAllHitsSuperDoc;
    }

    private int gatherAllHitsSuperDoc(int doc) throws IOException {
      reset();
      primeDoc = getPrimeDoc(doc);
      nextPrimeDoc = getNextPrimeDoc(doc);
      numDocs = nextPrimeDoc - primeDoc;
      float currentDocScore = 0;
      while (doc < nextPrimeDoc) {
        currentDocScore = scorer.score();
        aggregateScore += currentDocScore;
        if (currentDocScore > bestScore) {
          bestScore = currentDocScore;
        }
        hitsInEntity++;
        doc = scorer.nextDoc();
      }
      return primeDoc;
    }

    private void reset() {
      numDocs = 0;
      bestScore = 0;
      aggregateScore = 0;
      hitsInEntity = 0;
    }

    private int getNextPrimeDoc(int doc) {
      int nextSetBit = bitSet.nextSetBit(doc + 1);
      return nextSetBit == -1 ? NO_MORE_DOCS : nextSetBit;
    }

    private int getPrimeDoc(int doc) {
      if (bitSet.fastGet(doc)) {
        return doc;
      }
      return bitSet.prevSetBit(doc);
    }

    private boolean isScorerExhausted(int doc) {
      return doc == NO_MORE_DOCS ? true : false;
    }
  }
}
