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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

public class SlowQuery extends Query {

  private Query query;
  private boolean rewritten = false;

  public SlowQuery(Query query) {
    this.query = query;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    Weight weight = query.createWeight(searcher);
    return new SlowWeight(this, weight);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (!rewritten) {
      query = query.rewrite(reader);
    }
    return this;
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  public static class SlowWeight extends Weight {

    private final Weight weight;
    private final Query query;

    public SlowWeight(Query query, Weight weight) {
      this.query = query;
      this.weight = weight;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      return weight.explain(context, doc);
    }

    @Override
    public Query getQuery() {
      return query;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return weight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
      Scorer scorer = weight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      if (scorer == null) {
        highCpuWait(1);
        return null;
      }
      return new SlowScorer(weight, scorer);
    }

  }

  public static class SlowScorer extends Scorer {

    private final Scorer scorer;

    protected SlowScorer(Weight weight, Scorer scorer) {
      super(weight);
      this.scorer = scorer;
    }

    public int docID() {
      return scorer.docID();
    }

    public int nextDoc() throws IOException {
      highCpuWait(1);
      return scorer.nextDoc();
    }

    public int advance(int target) throws IOException {
      highCpuWait(1);
      return scorer.advance(target);
    }

    public float score() throws IOException {
      return scorer.score();
    }

    public int freq() throws IOException {
      return scorer.freq();
    }

  }

  public static void highCpuWait(long ms) {

  }

}
