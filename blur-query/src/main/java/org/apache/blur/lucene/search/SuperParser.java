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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public class SuperParser extends BlurQueryParser {

  private static final String MUST_NOT_STRING = "-";
  private static final String MUST_STRING = "+";
  private static final Pattern PATTERN = Pattern.compile("([-+]{0,1})\\s*?super\\s*?\\:\\s*?\\<(.*?)\\>");
  private static final Pattern CHECK = Pattern.compile("super\\s*?\\:\\s*?\\<");
  private static final String SEP = ".";
  private final boolean _superSearch;
  private final Filter _queryFilter;
  private final ScoreType _scoreType;
  private final Version _matchVersion;
  private final Term _defaultPrimeDocTerm;

  public SuperParser(Version matchVersion, BlurAnalyzer a, boolean superSearch, Filter queryFilter,
      ScoreType scoreType, Term defaultPrimeDocTerm) {
    super(matchVersion, SUPER, a, null);
    _matchVersion = matchVersion;
    _superSearch = superSearch;
    _queryFilter = queryFilter;
    _scoreType = scoreType;
    _defaultPrimeDocTerm = defaultPrimeDocTerm;
    setAutoGeneratePhraseQueries(true);
    setAllowLeadingWildcard(true);
  }

  @Override
  public Query parse(String query) throws ParseException {
    Matcher matcher = PATTERN.matcher(query);
    BooleanQuery booleanQuery = null;
    int lastEnd = -1;
    while (matcher.find()) {
      int count = matcher.groupCount();
      int start = matcher.start();
      int end = matcher.end();

      booleanQuery = addExtraQueryInfo(query, booleanQuery, lastEnd, start, end);

      for (int i = 0; i < count; i++) {
        String occurString = matcher.group(i + 1);
        i++;
        String superQueryStr = matcher.group(i + 1);
        Matcher matcherCheck = CHECK.matcher(superQueryStr);
        if (matcherCheck.find()) {
          throw new ParseException("Embedded super queries are not allowed [" + query + "].");
        }

        // Adding clause
        if (booleanQuery == null) {
          booleanQuery = new BooleanQuery();
        }
        Occur occur = getOccur(occurString);
        QueryParser parser = getNewParser();
        Query subQuery = parser.parse(superQueryStr);
        if (!isSameGroupName(subQuery)) {
          throw new ParseException("Super query [" + occurString + superQueryStr
              + "] cannot reference more than one column family.");
        }
        booleanQuery.add(newSuperQuery(subQuery), occur);
        lastEnd = end;
      }
    }
    booleanQuery = addExtraQueryInfo(query, booleanQuery, lastEnd);
    Query result = booleanQuery;
    if (result == null) {
      result = reprocess(super.parse(query));
    }
    return result;
  }

  private boolean isSameGroupName(Query query) {
    if (query instanceof BooleanQuery) {
      return isSameGroupName((BooleanQuery) query);
    }
    return true;
  }

  private BooleanQuery addExtraQueryInfo(String query, BooleanQuery booleanQuery, int lastEnd, int start, int end)
      throws ParseException {
    if (lastEnd != -1 && start != lastEnd) {
      // there was text inbetween the matches
      String missingMatch = query.substring(lastEnd, start);
      booleanQuery = addExtraQueryInfo(booleanQuery, missingMatch);
    } else if (lastEnd == -1 && start != 0) {
      // this means there was text in front of the first super query
      String missingMatch = query.substring(0, start);
      booleanQuery = addExtraQueryInfo(booleanQuery, missingMatch);
    }
    return booleanQuery;
  }

  private BooleanQuery addExtraQueryInfo(String query, BooleanQuery booleanQuery, int lastEnd) throws ParseException {
    if (lastEnd != -1 && lastEnd < query.length()) {
      // there was text at the end
      String missingMatch = query.substring(lastEnd);
      booleanQuery = addExtraQueryInfo(booleanQuery, missingMatch);
    }
    return booleanQuery;
  }

  private BooleanQuery addExtraQueryInfo(BooleanQuery booleanQuery, String missingMatch) throws ParseException {
    if (missingMatch.trim().isEmpty()) {
      return booleanQuery;
    }
    // Adding clause
    if (booleanQuery == null) {
      booleanQuery = new BooleanQuery();
    }
    QueryParser parser = getNewParser();
    Query subQuery = parser.parse(missingMatch);
    if (subQuery instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) subQuery;
      for (BooleanClause clause : bq) {
        booleanQuery.add(newSuperQuery(clause.getQuery()), clause.getOccur());
      }
    } else {
      booleanQuery.add(newSuperQuery(subQuery), Occur.SHOULD);
    }
    return booleanQuery;
  }

  private QueryParser getNewParser() {
    return new BlurQueryParser(_matchVersion, SUPER, _blurAnalyzer,_fieldNames);
  }

  private Occur getOccur(String occurString) {
    if (occurString.equals(MUST_STRING)) {
      return Occur.MUST;
    }
    if (occurString.equals(MUST_NOT_STRING)) {
      return Occur.MUST_NOT;
    }
    return Occur.SHOULD;
  }

  private SuperQuery newSuperQuery(Query query) {
    return new SuperQuery(wrapFilter(query), _scoreType, _defaultPrimeDocTerm);
  }

  private Query wrapFilter(Query query) {
    if (_queryFilter == null) {
      return query;
    }
    return new FilteredQuery(query, _queryFilter);
  }

  private boolean isSameGroupName(BooleanQuery booleanQuery) {
    String groupName = findFirstGroupName(booleanQuery);
    if (groupName == null) {
      return false;
    }
    return isSameGroupName(booleanQuery, groupName);
  }

  private boolean isSameGroupName(Query query, String groupName) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause clause : booleanQuery.clauses()) {
        if (!isSameGroupName(clause.getQuery(), groupName)) {
          return false;
        }
      }
      return true;
    } else {
      String fieldName = _fieldNames.get(query);
      String currentGroupName = getGroupName(fieldName);
      if (groupName.equals(currentGroupName)) {
        return true;
      }
      return false;
    }
  }

  private String getGroupName(String fieldName) {
    if (fieldName == null) {
      return null;
    }
    int index = fieldName.indexOf(SEP);
    if (index < 0) {
      return null;
    }
    return fieldName.substring(0, index);
  }

  private String findFirstGroupName(Query query) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause clause : booleanQuery.clauses()) {
        return findFirstGroupName(clause.getQuery());
      }
      return null;
    } else if (query instanceof SuperQuery) {
      SuperQuery sq = (SuperQuery) query;
      return findFirstGroupName(sq.getQuery());
    } else {
      String fieldName = _fieldNames.get(query);
      return getGroupName(fieldName);
    }
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
    } else {
      return newSuperQuery(query);
    }
  }

  public boolean isSuperSearch() {
    return _superSearch;
  }
}
