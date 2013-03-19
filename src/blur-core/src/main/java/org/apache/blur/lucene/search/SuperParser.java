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
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.analysis.BlurAnalyzer.TYPE;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class SuperParser extends QueryParser {

  private final Map<Query, String> fieldNames = new HashMap<Query, String>();
  private final boolean superSearch;
  private final Filter queryFilter;
  private final ScoreType scoreType;
  private final BlurAnalyzer blurAnalyzer;

  public SuperParser(Version matchVersion, BlurAnalyzer a, boolean superSearch, Filter queryFilter, ScoreType scoreType) {
    super(matchVersion, SUPER, a);
    this.setAutoGeneratePhraseQueries(true);
    this.setAllowLeadingWildcard(true);
    this.superSearch = superSearch;
    this.queryFilter = queryFilter;
    this.scoreType = scoreType;
    this.blurAnalyzer = a;
  }

  @Override
  public Query parse(String query) throws ParseException {
    return reprocess(super.parse(query));
  }

  @Override
  protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
    String field = term.field();
    TYPE type = blurAnalyzer.getTypeLookup(field);
    if (type != TYPE.DEFAULT) {
      throw new RuntimeException("Field [" + field + "] is type [" + type + "] which does not support fuzzy queries.");
    }
    return addField(super.newFuzzyQuery(term, minimumSimilarity, prefixLength), term.field());
  }

  @Override
  protected Query newMatchAllDocsQuery() {
    return addField(super.newMatchAllDocsQuery(), UUID.randomUUID().toString());
  }

  @Override
  protected MultiPhraseQuery newMultiPhraseQuery() {
    return new MultiPhraseQuery() {

      @Override
      public void add(Term[] terms, int position) {
        super.add(terms, position);
        for (Term term : terms) {
          addField(this, term.field());
        }
      }
    };
  }

  @Override
  protected PhraseQuery newPhraseQuery() {
    return new PhraseQuery() {

      @Override
      public void add(Term term, int position) {
        super.add(term, position);
        addField(this, term.field());
      }
    };
  }

  @Override
  protected Query newPrefixQuery(Term prefix) {
    String field = prefix.field();
    TYPE type = blurAnalyzer.getTypeLookup(field);
    if (type != TYPE.DEFAULT) {
      throw new RuntimeException("Field [" + field + "] is type [" + type + "] which does not support prefix queries.");
    }
    return addField(super.newPrefixQuery(prefix), field);
  }

  @Override
  protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    Query q = blurAnalyzer.getNewRangeQuery(field, part1, part2, startInclusive, endInclusive);
    if (q != null) {
      return addField(q, field);
    }
    return addField(super.newRangeQuery(field, part1, part2, startInclusive, endInclusive), field);
  }

  @Override
  protected Query newTermQuery(Term term) {
    String field = term.field();
    Query q = blurAnalyzer.getNewRangeQuery(field, term.text(), term.text(), true, true);
    if (q != null) {
      return addField(q, field);
    }
    return addField(super.newTermQuery(term), field);
  }

  @Override
  protected Query newWildcardQuery(Term t) {
    if (SUPER.equals(t.field()) && "*".equals(t.text())) {
      return new MatchAllDocsQuery();
    }
    String field = t.field();
    TYPE type = blurAnalyzer.getTypeLookup(field);
    if (type != TYPE.DEFAULT) {
      throw new RuntimeException("Field [" + field + "] is type [" + type + "] which does not support wildcard queries.");
    }
    return addField(super.newWildcardQuery(t), t.field());
  }

  private Query reprocess(Query query) {
    if (query == null || !isSuperSearch()) {
      return wrapFilter(query);
    }
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      List<BooleanClause> clauses = booleanQuery.clauses();
      for (BooleanClause bc : clauses) {
        Query q = bc.getQuery();
        bc.setQuery(newSuperQuery(q));
      }
      return booleanQuery;

      // if (isSameGroupName(booleanQuery)) {
      // return newSuperQuery(query);
      // } else {
      // List<BooleanClause> clauses = booleanQuery.clauses();
      // for (BooleanClause clause : clauses) {
      // clause.setQuery(reprocess(clause.getQuery()));
      // }
      // return booleanQuery;
      // }
    } else {
      return newSuperQuery(query);
    }
  }

  private SuperQuery newSuperQuery(Query query) {
    return new SuperQuery(wrapFilter(query), scoreType);
  }

  private Query wrapFilter(Query query) {
    if (queryFilter == null) {
      return query;
    }
    return new FilteredQuery(query, queryFilter);
  }

  // private boolean isSameGroupName(BooleanQuery booleanQuery) {
  // String groupName = findFirstGroupName(booleanQuery);
  // if (groupName == null) {
  // return false;
  // }
  // return isSameGroupName(booleanQuery, groupName);
  // }
  //
  // private boolean isSameGroupName(Query query, String groupName) {
  // if (query instanceof BooleanQuery) {
  // BooleanQuery booleanQuery = (BooleanQuery) query;
  // for (BooleanClause clause : booleanQuery.clauses()) {
  // if (!isSameGroupName(clause.getQuery(), groupName)) {
  // return false;
  // }
  // }
  // return true;
  // } else {
  // String fieldName = fieldNames.get(query);
  // String currentGroupName = getGroupName(fieldName);
  // if (groupName.equals(currentGroupName)) {
  // return true;
  // }
  // return false;
  // }
  // }
  //
  // private String getGroupName(String fieldName) {
  // if (fieldName == null) {
  // return null;
  // }
  // int index = fieldName.indexOf(SEP);
  // if (index < 0) {
  // return null;
  // }
  // return fieldName.substring(0, index);
  // }
  //
  // private String findFirstGroupName(Query query) {
  // if (query instanceof BooleanQuery) {
  // BooleanQuery booleanQuery = (BooleanQuery) query;
  // for (BooleanClause clause : booleanQuery.clauses()) {
  // return findFirstGroupName(clause.getQuery());
  // }
  // return null;
  // } else {
  // String fieldName = fieldNames.get(query);
  // return getGroupName(fieldName);
  // }
  // }

  private Query addField(Query q, String field) {
    fieldNames.put(q, field);
    return q;
  }

  public boolean isSuperSearch() {
    return superSearch;
  }
}
