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
import java.io.StringReader;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
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

  protected final Locale _locale = Locale.getDefault();
  protected final TimeZone _timeZone = TimeZone.getDefault();
  protected final boolean _allowLeadingWildcard;
  protected final int _fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;

  public BlurQueryParser(Version matchVersion, String f, Map<Query, String> fieldNames, FieldManager fieldManager) {
    super(matchVersion, f, fieldManager.getAnalyzerForQuery());
    _fieldNames = fieldNames == null ? new HashMap<Query, String>() : fieldNames;
    _fieldManager = fieldManager;
    _allowLeadingWildcard = true;
    setAllowLeadingWildcard(_allowLeadingWildcard);
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

  protected void customQueryCheck(String field) {
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
        return addField(_fieldManager.getCustomQuery(resolvedField, term.text()), resolvedField);
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
        return addField(new MatchAllDocsQuery(), fieldName);
      } else {
        String resolvedField = _fieldManager.resolveField(t.field());
        return addField(new TermQuery(new Term(BlurConstants.FIELDS, fieldName)), resolvedField);
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

  protected Query addField(Query q, String field) {
    _fieldNames.put(q, field);
    return q;
  }

  protected String analyzeField(String field, String text) throws ParseException {
    try {
      FieldTypeDefinition fieldTypeDefinition = _fieldManager.getFieldTypeDefinition(field);
      if (fieldTypeDefinition == null) {
        return text;
      }
      Analyzer analyzerForQuery = fieldTypeDefinition.getAnalyzerForQuery(field);
      if (analyzerForQuery instanceof KeywordAnalyzer) {
        return text;
      }

      StringBuilder builder = new StringBuilder();
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (isSpecialChar(c) && !isEscaped(text, i - 1)) {
          if (builder.length() > 0) {
            result.append(analyze(field, builder.toString(), analyzerForQuery));
            builder.setLength(0);
          }
          if (isSpecialRange(c)) {
            char closingChar = getClosingChar(c);
            int indexOf = text.indexOf(closingChar, i);
            if (indexOf < 0) {
              throw new ParseException("Could not find closing char [" + closingChar + "] in text [" + text + "]");
            }
            String s = text.substring(i, indexOf + 1);
            result.append(s);
            i += s.length() - 1;
          } else {
            result.append(c);
          }
        } else {
          builder.append(c);
        }
      }
      if (builder.length() > 0) {
        result.append(analyze(field, builder.toString(), analyzerForQuery));
        builder.setLength(0);
      }
      return result.toString();
    } catch (IOException e) {
      throw new ParseException(e.getMessage());
    }
  }

  private char getClosingChar(char c) throws ParseException {
    switch (c) {
    case '[':
      return ']';
    default:
      throw new ParseException("Closing char for " + c + " not found.");
    }
  }

  private boolean isSpecialRange(char c) {
    switch (c) {
    case '[':
      return true;
    case '{':
      return true;
    default:
      return false;
    }
  }

  private boolean isSpecialChar(char c) {
    switch (c) {
    case '?':
    case '/':
    case '[':
    case ']':
    case '}':
    case '{':
    case '*':
      return true;
    default:
      return false;
    }
  }

  private boolean isEscaped(String text, int pos) {
    if (pos <= 0) {
      return false;
    }
    return text.charAt(pos) == '\\';
  }

  private String analyze(String field, String text, Analyzer analyzerForQuery) throws IOException, ParseException {
    StringBuilder result = new StringBuilder();
    TokenStream tokenStream = analyzerForQuery.tokenStream(field, new StringReader(text));
    CharTermAttribute termAttribute = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.reset();
    if (tokenStream.incrementToken()) {
      result.append(termAttribute.toString());
    }
    if (tokenStream.incrementToken()) {
      throw new ParseException("Should not have multiple tokens in text [" + text + "] for field [" + field + "].");
    }
    return result.toString();
  }

  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
      throws ParseException {
    part1 = part1 == null ? null : analyzeField(field, part1);
    part2 = part2 == null ? null : analyzeField(field, part2);

    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, _locale);
    df.setLenient(true);
    DateTools.Resolution resolution = getDateResolution(field);

    try {
      part1 = DateTools.dateToString(df.parse(part1), resolution);
    } catch (Exception e) {
    }

    try {
      Date d2 = df.parse(part2);
      if (endInclusive) {
        // The user can only specify the date, not the time, so make sure
        // the time is set to the latest possible time of that date to really
        // include all documents:
        Calendar cal = Calendar.getInstance(_timeZone, _locale);
        cal.setTime(d2);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        d2 = cal.getTime();
      }
      part2 = DateTools.dateToString(d2, resolution);
    } catch (Exception e) {
    }
    return newRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    if ("*".equals(field)) {
      if ("*".equals(termStr)) {
        return newMatchAllDocsQuery();
      }
    }
    if (!_allowLeadingWildcard && (termStr.startsWith("*") || termStr.startsWith("?"))) {
      throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");
    }
    if (!"*".equals(termStr)) {
      termStr = analyzeField(field, termStr);
    }
    Term t = new Term(field, termStr);
    return newWildcardQuery(t);
  }

  @Override
  protected Query getRegexpQuery(String field, String termStr) throws ParseException {
    termStr = analyzeField(field, termStr);
    Term t = new Term(field, termStr);
    return newRegexpQuery(t);
  }

  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    if (!_allowLeadingWildcard && termStr.startsWith("*"))
      throw new ParseException("'*' not allowed as first character in PrefixQuery");
    termStr = analyzeField(field, termStr);
    Term t = new Term(field, termStr);
    return newPrefixQuery(t);
  }

  @Override
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
    termStr = analyzeField(field, termStr);
    Term t = new Term(field, termStr);
    return newFuzzyQuery(t, minSimilarity, _fuzzyPrefixLength);
  }

}
