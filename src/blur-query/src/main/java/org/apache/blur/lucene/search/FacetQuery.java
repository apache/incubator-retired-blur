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
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

public class FacetQuery extends AbstractWrapperQuery {

  private Query[] facets;
  private AtomicLongArray counts;

  public FacetQuery(Query query, Query[] facets, AtomicLongArray counts) {
    super(query, false);
    this.facets = facets;
    this.counts = counts;
  }

  public FacetQuery(Query query, Query[] facets, AtomicLongArray counts, boolean rewritten) {
    super(query, rewritten);
    this.facets = facets;
    this.counts = counts;
  }

  public String toString() {
    return "facet:{" + _query.toString() + "}";
  }

  public String toString(String field) {
    return "facet:{" + _query.toString(field) + "}";
  }

  @Override
  public Query clone() {
    return new FacetQuery((Query) _query.clone(), facets, counts, _rewritten);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (_rewritten) {
      return this;
    }
    for (int i = 0; i < facets.length; i++) {
      facets[i] = facets[i].rewrite(reader);
    }
    return new FacetQuery(_query.rewrite(reader), facets, counts, true);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    Weight weight = _query.createWeight(searcher);
    return new FacetWeight(weight, getWeights(searcher), counts);
  }

  private Weight[] getWeights(IndexSearcher searcher) throws IOException {
    Weight[] weights = new Weight[facets.length];
    for (int i = 0; i < weights.length; i++) {
      weights[i] = facets[i].createWeight(searcher);
    }
    return weights;
  }

  public static class FacetWeight extends Weight {

    private Weight weight;
    private Weight[] facets;
    private AtomicLongArray counts;

    public FacetWeight(Weight weight, Weight[] facets, AtomicLongArray counts) {
      this.weight = weight;
      this.facets = facets;
      this.counts = counts;
    }

    @Override
    public Explanation explain(AtomicReaderContext reader, int doc) throws IOException {
      return weight.explain(reader, doc);
    }

    @Override
    public Query getQuery() {
      return weight.getQuery();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
      Scorer scorer = weight.scorer(context, true, topScorer, acceptDocs);
      if (scorer == null) {
        return null;
      }
      return new FacetScorer(scorer, getScorers(context, true, topScorer, acceptDocs), counts);
    }

    private Scorer[] getScorers(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
      Scorer[] scorers = new Scorer[facets.length];
      for (int i = 0; i < scorers.length; i++) {
        scorers[i] = facets[i].scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      }
      return scorers;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return weight.getValueForNormalization();
    }
  }

  public static class FacetScorer extends Scorer {

    private Scorer baseScorer;
    private Scorer[] facets;
    private AtomicLongArray counts;
    private int facetLength;

    public FacetScorer(Scorer scorer, Scorer[] facets, AtomicLongArray counts) {
      super(scorer.getWeight());
      this.baseScorer = scorer;
      this.facets = facets;
      this.counts = counts;
      this.facetLength = facets.length;
    }

    private int processFacets(int doc) throws IOException {
      if (doc == NO_MORE_DOCS) {
        return doc;
      }
      for (int i = 0; i < facetLength; i++) {
        Scorer facet = facets[i];
        if (facet == null) {
          continue;
        }
        int docID = facet.docID();
        if (docID == NO_MORE_DOCS) {
          continue;
        }
        if (docID == doc) {
          counts.incrementAndGet(i);
        } else if (docID < doc) {
          if (facet.advance(doc) == doc) {
            counts.incrementAndGet(i);
          }
        }
      }
      return doc;
    }

    @Override
    public float score() throws IOException {
      return baseScorer.score();
    }

    @Override
    public int advance(int target) throws IOException {
      return processFacets(baseScorer.advance(target));
    }

    @Override
    public int docID() {
      return baseScorer.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return processFacets(baseScorer.nextDoc());
    }

    @Override
    public int freq() throws IOException {
      return baseScorer.freq();
    }

    @Override
    public long cost() {
      return baseScorer.cost();
    }
  }
}
