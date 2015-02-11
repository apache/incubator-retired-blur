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
package org.apache.blur.server;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import lucene.security.index.AccessControlFactory;
import lucene.security.search.SecureIndexSearcher;

import org.apache.blur.lucene.search.SuperQuery;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

public class BlurSecureIndexSearcher extends SecureIndexSearcher {

  public BlurSecureIndexSearcher(IndexReader r, ExecutorService executor, AccessControlFactory accessControlFactory,
      Collection<String> readAuthorizations, Collection<String> discoverAuthorizations, Set<String> discoverableFields)
      throws IOException {
    super(r, executor, accessControlFactory, readAuthorizations, discoverAuthorizations, discoverableFields);
  }

  /**
   * This method is very important!!! It handles rewriting the real query (which
   * can be a {@link SuperQuery} to have document (record) level filtering or
   * access control.
   */
  @Override
  protected Query wrapFilter(Query query, Filter filter) {
    if (filter == null) {
      return query;
    } else if (query instanceof SuperQuery) {
      SuperQuery superQuery = (SuperQuery) query;
      Query innerQuery = superQuery.getQuery();
      Term primeDocTerm = superQuery.getPrimeDocTerm();
      ScoreType scoreType = superQuery.getScoreType();
      return new SuperQuery(wrapFilter(innerQuery, filter), scoreType, primeDocTerm);
    } else if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      List<BooleanClause> clauses = booleanQuery.clauses();
      for (BooleanClause booleanClause : clauses) {
        booleanClause.setQuery(wrapFilter(booleanClause.getQuery(), filter));
      }
      return booleanQuery;
    } else {
      return new FilteredQuery(query, filter);
    }
  }

}
