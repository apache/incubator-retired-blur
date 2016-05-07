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
import java.util.Arrays;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;

public class FacetQuery extends AbstractWrapperQuery {

  private static final Log LOG = LogFactory.getLog(FacetQuery.class);

  private final Query[] _facets;
  private final FacetExecutor _executor;

  public FacetQuery(Query query, Query[] facets, FacetExecutor executor) {
    super(query, false);
    _facets = facets;
    _executor = executor;
  }

  public FacetQuery(Query query, Query[] facets, FacetExecutor executor, boolean rewritten) {
    super(query, rewritten);
    _facets = facets;
    _executor = executor;
  }

  public String toString() {
    return "facet:{" + _query.toString() + "}";
  }

  public String toString(String field) {
    return "facet:{" + _query.toString(field) + "}";
  }

  public Query[] getFacets() {
    return _facets;
  }

  @Override
  public Query clone() {
    return new FacetQuery((Query) _query.clone(), _facets, _executor, _rewritten);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (_rewritten) {
      return this;
    }
    Query[] facets = new Query[_facets.length];
    for (int i = 0; i < _facets.length; i++) {
      facets[i] = _facets[i].rewrite(reader);
    }
    return new FacetQuery(_query.rewrite(reader), facets, _executor, true);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    Weight weight = _query.createWeight(searcher);
    return new FacetWeight(weight, getWeights(searcher), _executor);
  }

  private Weight[] getWeights(IndexSearcher searcher) throws IOException {
    Weight[] weights = new Weight[_facets.length];
    for (int i = 0; i < weights.length; i++) {
      weights[i] = _facets[i].createWeight(searcher);
    }
    return weights;
  }

  public static class FacetWeight extends Weight {

    private final Weight _weight;
    private final Weight[] _facets;
    private FacetExecutor _executor;

    public FacetWeight(Weight weight, Weight[] facets, FacetExecutor executor) {
      _weight = weight;
      _facets = facets;
      _executor = executor;
    }

    @Override
    public Explanation explain(AtomicReaderContext reader, int doc) throws IOException {
      return _weight.explain(reader, doc);
    }

    @Override
    public Query getQuery() {
      return _weight.getQuery();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      _weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs)
        throws IOException {
      Scorer scorer = _weight.scorer(context, true, topScorer, acceptDocs);
      if (scorer == null) {
        return null;
      }
      if (!_executor.scorersAlreadyAdded(context)) {
        Scorer[] scorers = getScorers(context, true, topScorer, acceptDocs);
        LOG.debug(_executor.getPrefix("Adding scorers for context [{0}] scorers [{1}]"), context,
            Arrays.asList(scorers));
        _executor.addScorers(context, scorers);
      } else {
        LOG.debug(_executor.getPrefix("Scorers already added for context [{0}]"), context);
      }
      return new FacetScorer(scorer, _executor, context);
    }

    private Scorer[] getScorers(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer,
        Bits acceptDocs) throws IOException {
      Scorer[] scorers = new Scorer[_facets.length];
      for (int i = 0; i < scorers.length; i++) {
        scorers[i] = _facets[i].scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      }
      return scorers;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return _weight.getValueForNormalization();
    }
  }

  public static class FacetScorer extends Scorer {

    private static final Log LOG = LogFactory.getLog(FacetScorer.class);

    private final Scorer _baseScorer;
    private final OpenBitSet _hit;

    public FacetScorer(Scorer scorer, FacetExecutor executor, AtomicReaderContext context) throws IOException {
      super(scorer.getWeight());
      _baseScorer = scorer;
      _hit = executor.getBitSet(context);
    }

    private int processFacets(int doc) throws IOException {
      if (doc == NO_MORE_DOCS) {
        return doc;
      }
      if (doc < 0) {
        LOG.error("DocId from base scorer < 0 [{0}]", _baseScorer);
        return doc;
      }
      _hit.fastSet(doc);
      return doc;
    }

    @Override
    public float score() throws IOException {
      return _baseScorer.score();
    }

    @Override
    public int advance(int target) throws IOException {
      return processFacets(_baseScorer.advance(target));
    }

    @Override
    public int docID() {
      return _baseScorer.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return processFacets(_baseScorer.nextDoc());
    }

    @Override
    public int freq() throws IOException {
      return _baseScorer.freq();
    }

    @Override
    public long cost() {
      return _baseScorer.cost();
    }
  }
}
