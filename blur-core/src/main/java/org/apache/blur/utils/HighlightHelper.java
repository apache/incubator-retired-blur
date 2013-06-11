package org.apache.blur.utils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;

public class HighlightHelper {
  
  private static final Collection<String> FIELDS_NOT_TO_HIGHLIGHT = new HashSet<String>() {{
      add(BlurConstants.SUPER);
      add(BlurConstants.ROW_ID);
      add(BlurConstants.RECORD_ID);
      add(BlurConstants.PRIME_DOC);
      add(BlurConstants.FAMILY);
    }};

  /**
   * NOTE: This method will not preserve the correct field types. 
   */
  public static Document highlight(int docId, Document document, Query query, BlurAnalyzer analyzer, IndexReader reader)
      throws IOException, InvalidTokenOffsetsException {

    String preTag = "{{";
    String postTag = "}}";

    SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter(preTag, postTag);
    System.out.println(docId);
    System.out.println(document);
    Document result = new Document();
    for (IndexableField f : document) {
      String name = f.name();
      if (FIELDS_NOT_TO_HIGHLIGHT.contains(name)) {
        continue;
      }
      String text = f.stringValue();
      Number numericValue = f.numericValue();
      if (numericValue != null) {
        if (shouldNumberBeHighlighted(name, numericValue, query)) {
          String numberHighlight = preTag + text + postTag;
          result.add(new StringField(name, numberHighlight, Store.YES));
        }
      } else {
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query, name));
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
