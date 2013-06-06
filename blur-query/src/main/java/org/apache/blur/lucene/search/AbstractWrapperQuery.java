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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

public abstract class AbstractWrapperQuery extends Query {
  protected Query _query;
  protected boolean _rewritten;

  public AbstractWrapperQuery(Query query) {
    this(query, false);
  }

  public AbstractWrapperQuery(Query query, boolean rewritten) {
    this._query = query;
    this._rewritten = rewritten;
  }

  public Query getQuery() {
    return _query;
  }

  public boolean isRewritten() {
    return _rewritten;
  }

  public abstract Query clone();

  public abstract Weight createWeight(IndexSearcher searcher) throws IOException;

  public void extractTerms(Set<Term> terms) {
    _query.extractTerms(terms);
  }

  public float getBoost() {
    return _query.getBoost();
  }

  public Similarity getSimilarity(IndexSearcher searcher) {
    return searcher.getSimilarity();
  }

  public abstract Query rewrite(IndexReader reader) throws IOException;

  public void setBoost(float b) {
    _query.setBoost(b);
  }

  
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((_query == null) ? 0 : _query.hashCode());
    result = prime * result + (_rewritten ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    AbstractWrapperQuery other = (AbstractWrapperQuery) obj;
    if (_query == null) {
      if (other._query != null)
        return false;
    } else if (!_query.equals(other._query))
      return false;
    if (_rewritten != other._rewritten)
      return false;
    return true;
  }

  public abstract String toString();

  public abstract String toString(String field);

}
