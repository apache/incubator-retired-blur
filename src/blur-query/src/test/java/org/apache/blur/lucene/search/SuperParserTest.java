package org.apache.blur.lucene.search;

import static org.junit.Assert.*;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class SuperParserTest {

  private SuperParser parser;

  @Before
  public void setup() {
    parser = new SuperParser(Version.LUCENE_40, "", new WhitespaceAnalyzer(Version.LUCENE_40), ScoreType.SUPER, new Term("_primedoc_"));
  }

  @Test
  public void testParser1() throws ParseException {
    Query query = parser.parse(" +super:<a:a d:e b:b> ");

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term("a", "a")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("d", "e")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("b", "b")), Occur.SHOULD);
    SuperQuery superQuery = new SuperQuery(booleanQuery, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery bq = new BooleanQuery();
    bq.add(superQuery, Occur.MUST);
    
    assertEquals(bq, query);

  }

  @Test
  public void testParser2() throws ParseException {
    Query query = parser.parse("super:<c:c d:d>");

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term("c", "c")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("d", "d")), Occur.SHOULD);
    SuperQuery superQuery = new SuperQuery(booleanQuery, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery bq = new BooleanQuery();
    bq.add(superQuery, Occur.SHOULD);

    assertEquals(bq, query);
  }

  @Test
  public void testParser3() throws ParseException {
    Query query = parser.parse("a:a d:e b:b");

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term("a", "a")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("d", "e")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("b", "b")), Occur.SHOULD);

    assertEquals(booleanQuery, query);
  }

  @Test
  public void testParser4() throws ParseException {
    Query query = parser.parse("super:<a:a d:e b:b>  - super:<c:c d:d>");

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term("a", "a")), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("d", "e")), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("b", "b")), Occur.SHOULD);

    BooleanQuery booleanQuery2 = new BooleanQuery();
    booleanQuery2.add(new TermQuery(new Term("c", "c")), Occur.SHOULD);
    booleanQuery2.add(new TermQuery(new Term("d", "d")), Occur.SHOULD);

    SuperQuery superQuery1 = new SuperQuery(booleanQuery1, ScoreType.SUPER, new Term("_primedoc_"));
    SuperQuery superQuery2 = new SuperQuery(booleanQuery2, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(superQuery1, Occur.SHOULD);
    booleanQuery.add(superQuery2, Occur.MUST_NOT);

    assertEquals(booleanQuery, query);
  }

  @Test
  public void testParser5() throws ParseException {

    Query query = parser.parse("super:<a:a d:{e TO f} b:b test:hello\\<> - super:<c:c d:d>");

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term("a", "a")), Occur.SHOULD);
    booleanQuery1.add(new TermRangeQuery("d", new BytesRef("e"), new BytesRef("f"), false, false), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("b", "b")), Occur.SHOULD);
    // std analyzer took the "<" out
    booleanQuery1.add(new TermQuery(new Term("test", "hello<")), Occur.SHOULD);

    BooleanQuery booleanQuery2 = new BooleanQuery();
    booleanQuery2.add(new TermQuery(new Term("c", "c")), Occur.SHOULD);
    booleanQuery2.add(new TermQuery(new Term("d", "d")), Occur.SHOULD);

    SuperQuery superQuery1 = new SuperQuery(booleanQuery1, ScoreType.SUPER, new Term("_primedoc_"));
    SuperQuery superQuery2 = new SuperQuery(booleanQuery2, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(superQuery1, Occur.SHOULD);
    booleanQuery.add(superQuery2, Occur.MUST_NOT);

    assertEquals(booleanQuery, query);
  }

  @Test
  public void testParser6() throws ParseException {
    SuperParser parser = new SuperParser(Version.LUCENE_40, "", new StandardAnalyzer(Version.LUCENE_40), ScoreType.SUPER, new Term("_primedoc_"));
    try {
      parser.parse("super : <a:a d:{e TO d} b:b super:<test:hello\\<>> super:<c:c d:d>");
      fail();
    } catch (ParseException e) {
      // should throw an error
    }
  }
}
