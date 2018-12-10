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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class BlurQueryParser extends QueryParser {

  public static final String SUPER = "super";

  protected final Map<Query, String> _fieldNames;
  protected final FieldManager _fieldManager;

  public BlurQueryParser(Version matchVersion, String f, Map<Query, String> fieldNames, FieldManager fieldManager) {
    super(matchVersion, f, fieldManager.getAnalyzerForQuery());
    _fieldNames = fieldNames == null ? new HashMap<Query, String>() : fieldNames;
    _fieldManager = fieldManager;
    setAllowLeadingWildcard(true);
    setAutoGeneratePhraseQueries(true);
  }

  @Override
  protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
    String resolvedField = _fieldManager.resolveField(term.field());
    try {
      Boolean b = _fieldManager.checkSupportForFuzzyQuery(resolvedField);
      if (!(b == null || b)) {
        throw new RuntimeException("Field [" + resolvedField + "] is type ["
            + _fieldManager.getFieldTypeDefinition(resolvedField) + "] which does not support fuzzy queries.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    customQueryCheck(resolvedField);
    return addField(super.newFuzzyQuery(new Term(resolvedField, term.text()), minimumSimilarity, prefixLength),
        resolvedField);
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
          String resolvedField = _fieldManager.resolveField(term.field());
          customQueryCheck(resolvedField);
          addField(this, resolvedField);
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
        String resolvedField = _fieldManager.resolveField(term.field());
        customQueryCheck(resolvedField);
        addField(this, resolvedField);
      }
    };
  }

  @Override
  protected Query newPrefixQuery(Term prefix) {
    String resolvedField = _fieldManager.resolveField(prefix.field());
    try {
      Boolean b = _fieldManager.checkSupportForPrefixQuery(resolvedField);
      if (!(b == null || b)) {
        throw new RuntimeException("Field [" + resolvedField + "] is type ["
            + _fieldManager.getFieldTypeDefinition(resolvedField) + "] which does not support prefix queries.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    customQueryCheck(resolvedField);
    return addField(super.newPrefixQuery(new Term(resolvedField, prefix.text())), resolvedField);
  }

  @Override
  protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    String resolvedField = _fieldManager.resolveField(field);
    customQueryCheck(resolvedField);
    Query q;
    try {
      q = _fieldManager.getNewRangeQuery(resolvedField, part1, part2, startInclusive, endInclusive);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (q != null) {
      return addField(q, resolvedField);
    }
    return addField(super.newRangeQuery(resolvedField, part1, part2, startInclusive, endInclusive), resolvedField);
  }

  private void customQueryCheck(String field) {
    try {
      Boolean b = _fieldManager.checkSupportForCustomQuery(field);
      if (b != null && b) {
        throw new RuntimeException("Field [" + field + "] is type [" + _fieldManager.getFieldTypeDefinition(field)
            + "] queries should exist with \" around them.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Query newTermQuery(Term term) {
    String resolvedField = _fieldManager.resolveField(term.field());
    try {
      Boolean b = _fieldManager.checkSupportForCustomQuery(resolvedField);
      if (b != null && b) {
        return _fieldManager.getCustomQuery(resolvedField, term.text());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Query q;
    try {
      q = _fieldManager.getTermQueryIfNumeric(resolvedField, term.text());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (q != null) {
      return addField(q, resolvedField);
    }
    return addField(super.newTermQuery(new Term(resolvedField, term.text())), resolvedField);
  }

  @Override
  protected Query newWildcardQuery(Term t) {
    if ("*".equals(t.text())) {
      String fieldName = t.field();
      if (SUPER.equals(fieldName)) {
        return new MatchAllDocsQuery();
      } else {
        return new TermQuery(new Term(BlurConstants.FIELDS, fieldName));
      }
    }
    String resolvedField = _fieldManager.resolveField(t.field());
    try {
      Boolean b = _fieldManager.checkSupportForWildcardQuery(resolvedField);
      if (!(b == null || b)) {
        throw new RuntimeException("Field [" + resolvedField + "] is type ["
            + _fieldManager.getFieldTypeDefinition(resolvedField) + "] which does not support wildcard queries.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    customQueryCheck(resolvedField);
    return addField(super.newWildcardQuery(new Term(resolvedField, t.text())), resolvedField);
  }

  @Override
  protected Query newRegexpQuery(Term t) {
    String resolvedField = _fieldManager.resolveField(t.field());
    try {
      Boolean b = _fieldManager.checkSupportForRegexQuery(resolvedField);
      if (!(b == null || b)) {
        throw new RuntimeException("Field [" + resolvedField + "] is type ["
            + _fieldManager.getFieldTypeDefinition(resolvedField) + "] which does not support wildcard queries.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    customQueryCheck(resolvedField);
    return addField(super.newRegexpQuery(new Term(resolvedField, t.text())), resolvedField);
  }

  private Query addField(Query q, String field) {
    _fieldNames.put(q, field);
    return q;
  }

}
