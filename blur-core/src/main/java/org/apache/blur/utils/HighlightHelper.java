package org.apache.blur.utils;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.lucene.search.SuperQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.util.BytesRef;

public class HighlightHelper {

  private static final Collection<String> FIELDS_NOT_TO_HIGHLIGHT = new HashSet<String>() {
    private static final long serialVersionUID = 1L;
    {
      add(BlurConstants.ROW_ID);
      add(BlurConstants.RECORD_ID);
      add(BlurConstants.PRIME_DOC);
      add(BlurConstants.FAMILY);
    }
  };

  /**
   * NOTE: This method will not preserve the correct field types.
   * 
   * @param preTag
   * @param postTag
   */
  public static Document highlight(int docId, Document document, Query query, FieldManager fieldManager,
      IndexReader reader, String preTag, String postTag) throws IOException, InvalidTokenOffsetsException {

    String fieldLessFieldName = fieldManager.getFieldLessFieldName();

    Query fixedQuery = fixSuperQuery(query, null, fieldLessFieldName);

    Analyzer analyzer = fieldManager.getAnalyzerForQuery();

    SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter(preTag, postTag);
    Document result = new Document();
    for (IndexableField f : document) {
      String name = f.name();
      if (fieldLessFieldName.equals(name) || FIELDS_NOT_TO_HIGHLIGHT.contains(name)) {
        result.add(f);
        continue;
      }
      String text = f.stringValue();
      Number numericValue = f.numericValue();

      Query fieldFixedQuery;
      if (fieldManager.isFieldLessIndexed(name)) {
        fieldFixedQuery = fixSuperQuery(query, name, fieldLessFieldName);
      } else {
        fieldFixedQuery = fixedQuery;
      }

      if (numericValue != null) {
        if (shouldNumberBeHighlighted(name, numericValue, fieldFixedQuery)) {
          String numberHighlight = preTag + text + postTag;
          result.add(new StringField(name, numberHighlight, Store.YES));
        }
      } else {
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(fieldFixedQuery, name));
        TokenStream tokenStream = TokenSources.getAnyTokenStream(reader, docId, name, analyzer);
        TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, false, 10);
        for (int j = 0; j < frag.length; j++) {
          if ((frag[j] != null) && (frag[j].getScore() > 0)) {
            result.add(new StringField(name, frag[j].toString(), Store.YES));
          }
        }
      }
    }
    return result;
  }

  private static Query fixSuperQuery(Query query, String name, String fieldLessFieldName) {
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      BooleanQuery newBq = new BooleanQuery();
      for (BooleanClause booleanClause : bq) {
        newBq.add(fixSuperQuery(booleanClause.getQuery(), name, fieldLessFieldName), booleanClause.getOccur());
      }
      return newBq;
    } else if (query instanceof SuperQuery) {
      SuperQuery sq = (SuperQuery) query;
      return setFieldIfNeeded(sq.getQuery(), name, fieldLessFieldName);
    } else {
      return setFieldIfNeeded(query, name, fieldLessFieldName);
    }
  }

  private static Query setFieldIfNeeded(Query query, String name, String fieldLessFieldName) {
    if (name == null) {
      return query;
    }
    if (query instanceof TermQuery) {
      TermQuery tq = (TermQuery) query;
      Term term = tq.getTerm();
      if (term.field().equals(fieldLessFieldName)) {
        return new TermQuery(new Term(name, term.bytes()));
      }
    } else if (query instanceof WildcardQuery) {
      WildcardQuery wq = (WildcardQuery) query;
      Term term = wq.getTerm();
      if (term.field().equals(fieldLessFieldName)) {
        return new WildcardQuery(new Term(name, term.bytes()));
      }
    } else if (query instanceof MultiPhraseQuery) {
      MultiPhraseQuery mpq = (MultiPhraseQuery) query;
      int[] positions = mpq.getPositions();
      List<Term[]> termArrays = mpq.getTermArrays();
      if (isTermField(termArrays, fieldLessFieldName)) {
        MultiPhraseQuery multiPhraseQuery = new MultiPhraseQuery();
        multiPhraseQuery.setSlop(mpq.getSlop());
        for (int i = 0; i < termArrays.size(); i++) {
          multiPhraseQuery.add(changeFields(termArrays.get(i), name), positions[i]);
        }
        return multiPhraseQuery;
      }
    } else if (query instanceof PhraseQuery) {
      PhraseQuery pq = (PhraseQuery) query;
      Term[] terms = pq.getTerms();
      int[] positions = pq.getPositions();
      String field = terms[0].field();
      if (field.equals(BlurConstants.SUPER)) {
        PhraseQuery phraseQuery = new PhraseQuery();
        for (int i = 0; i < terms.length; i++) {
          phraseQuery.add(new Term(name, terms[i].bytes()), positions[i]);
        }
        phraseQuery.setSlop(pq.getSlop());
        return phraseQuery;
      }
    } else if (query instanceof PrefixQuery) {
      PrefixQuery pq = (PrefixQuery) query;
      Term term = pq.getPrefix();
      if (term.field().equals(BlurConstants.SUPER)) {
        return new PrefixQuery(new Term(name, term.bytes()));
      }
    } else if (query instanceof TermRangeQuery) {
      TermRangeQuery trq = (TermRangeQuery) query;
      BytesRef lowerTerm = trq.getLowerTerm();
      BytesRef upperTerm = trq.getUpperTerm();
      boolean includeUpper = trq.includesUpper();
      boolean includeLower = trq.includesLower();
      String field = trq.getField();
      if (field.equals(BlurConstants.SUPER)) {
        return new TermRangeQuery(name, lowerTerm, upperTerm, includeLower, includeUpper);
      }
    }
    return query;
  }

  private static Term[] changeFields(Term[] terms, String name) {
    Term[] newTerms = new Term[terms.length];
    for (int i = 0; i < terms.length; i++) {
      newTerms[i] = new Term(name, terms[i].bytes());
    }
    return newTerms;
  }

  private static boolean isTermField(List<Term[]> termArrays, String fieldName) {
    Term[] terms = termArrays.get(0);
    return terms[0].field().equals(fieldName);
  }

  public static boolean shouldNumberBeHighlighted(String name, Number numericValue, Query query) {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      for (BooleanClause booleanClause : booleanQuery) {
        if (booleanClause.isProhibited()) {
          continue;
        } else {
          if (shouldNumberBeHighlighted(name, numericValue, booleanClause.getQuery())) {
            return true;
          }
        }
      }
    } else {
      if (query instanceof NumericRangeQuery) {
        if (numericValue instanceof Integer) {
          return checkInteger(name, numericValue, query);
        } else if (numericValue instanceof Double) {
          return checkDouble(name, numericValue, query);
        } else if (numericValue instanceof Float) {
          return checkFloat(name, numericValue, query);
        } else if (numericValue instanceof Long) {
          return checkLong(name, numericValue, query);
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkLong(String name, Number numericValue, Query query) {
    long value = (Long) numericValue;
    NumericRangeQuery<Long> nrq = (NumericRangeQuery<Long>) query;
    if (!name.equals(nrq.getField())) {
      return false;
    }
    if (nrq.includesMin()) {
      if (nrq.includesMax()) {
        if (value >= nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value >= nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    } else {
      if (nrq.includesMax()) {
        if (value > nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value > nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkFloat(String name, Number numericValue, Query query) {
    float value = (Float) numericValue;
    NumericRangeQuery<Float> nrq = (NumericRangeQuery<Float>) query;
    if (!name.equals(nrq.getField())) {
      return false;
    }
    if (nrq.includesMin()) {
      if (nrq.includesMax()) {
        if (value >= nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value >= nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    } else {
      if (nrq.includesMax()) {
        if (value > nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value > nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkDouble(String name, Number numericValue, Query query) {
    double value = (Double) numericValue;
    NumericRangeQuery<Double> nrq = (NumericRangeQuery<Double>) query;
    if (!name.equals(nrq.getField())) {
      return false;
    }
    if (nrq.includesMin()) {
      if (nrq.includesMax()) {
        if (value >= nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value >= nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    } else {
      if (nrq.includesMax()) {
        if (value > nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value > nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkInteger(String name, Number numericValue, Query query) {
    int value = (Integer) numericValue;
    NumericRangeQuery<Integer> nrq = (NumericRangeQuery<Integer>) query;
    if (!name.equals(nrq.getField())) {
      return false;
    }
    if (nrq.includesMin()) {
      if (nrq.includesMax()) {
        if (value >= nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value >= nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    } else {
      if (nrq.includesMax()) {
        if (value > nrq.getMin() && value <= nrq.getMax()) {
          return true;
        }
      } else {
        if (value > nrq.getMin() && value < nrq.getMax()) {
          return true;
        }
      }
    }
    return false;
  }
}
