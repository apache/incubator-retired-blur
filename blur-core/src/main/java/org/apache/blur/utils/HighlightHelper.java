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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.SuperQuery;
import org.apache.blur.thrift.generated.Selector;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;

public class HighlightHelper {

  private static final Log LOG = LogFactory.getLog(HighlightHelper.class);

  private static final Collection<String> FIELDS_NOT_TO_HIGHLIGHT = new HashSet<String>() {
    {
      add(BlurConstants.SUPER);
      add(BlurConstants.ROW_ID);
      add(BlurConstants.RECORD_ID);
      add(BlurConstants.PRIME_DOC);
      add(BlurConstants.FAMILY);
    }
  };

  public static List<Document> highlightDocuments(IndexReader reader, Term term,
      ResetableDocumentStoredFieldVisitor fieldSelector, Selector selector, Query highlightQuery,
      BlurAnalyzer analyzer, String preTag, String postTag) throws IOException {
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    int docFreq = reader.docFreq(term);
    BooleanQuery booleanQueryForFamily = null;
    BooleanQuery booleanQuery = null;
    if (selector.getColumnFamiliesToFetchSize() > 0) {
      booleanQueryForFamily = new BooleanQuery();
      for (String familyName : selector.getColumnFamiliesToFetch()) {
        booleanQueryForFamily
            .add(new TermQuery(new Term(BlurConstants.FAMILY, familyName)), BooleanClause.Occur.SHOULD);
      }
      booleanQuery = new BooleanQuery();
      booleanQuery.add(new TermQuery(term), BooleanClause.Occur.MUST);
      booleanQuery.add(booleanQueryForFamily, BooleanClause.Occur.MUST);
    }
    Query query = booleanQuery == null ? new TermQuery(term) : booleanQuery;
    TopDocs topDocs = indexSearcher.search(query, docFreq);
    int totalHits = topDocs.totalHits;
    List<Document> docs = new ArrayList<Document>();

    int start = selector.getStartRecord();
    int end = selector.getMaxRecordsToFetch() + start;

    for (int i = start; i < end; i++) {
      if (i >= totalHits) {
        break;
      }
      int doc = topDocs.scoreDocs[i].doc;
      indexSearcher.doc(doc, fieldSelector);
      Document document = fieldSelector.getDocument();
      try {
        document = highlight(doc, document, highlightQuery, analyzer, reader, preTag, postTag);
      } catch (InvalidTokenOffsetsException e) {
        LOG.error("Unknown error while tring to highlight", e);
      }
      docs.add(document);
      fieldSelector.reset();
    }
    return docs;
  }

  /**
   * NOTE: This method will not preserve the correct field types.
   * 
   * @param preTag
   * @param postTag
   */
  public static Document highlight(int docId, Document document, Query query, BlurAnalyzer analyzer,
      IndexReader reader, String preTag, String postTag) throws IOException, InvalidTokenOffsetsException {

    Query fixedQuery = fixSuperQuery(query);

    SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter(preTag, postTag);
    Document result = new Document();
    for (IndexableField f : document) {
      String name = f.name();
      if (FIELDS_NOT_TO_HIGHLIGHT.contains(name)) {
        result.add(f);
        continue;
      }
      String text = f.stringValue();
      Number numericValue = f.numericValue();
      if (numericValue != null) {
        if (shouldNumberBeHighlighted(name, numericValue, fixedQuery)) {
          String numberHighlight = preTag + text + postTag;
          result.add(new StringField(name, numberHighlight, Store.YES));
        }
      } else {
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(fixedQuery, name));
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

  private static Query fixSuperQuery(Query query) {
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      for (BooleanClause booleanClause : bq) {
        booleanClause.setQuery(fixSuperQuery(booleanClause.getQuery()));
      }
      return bq;
    } else if (query instanceof SuperQuery) {
      SuperQuery sq = (SuperQuery) query;
      return sq.getQuery();
    } else {
      return query;
    }
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
