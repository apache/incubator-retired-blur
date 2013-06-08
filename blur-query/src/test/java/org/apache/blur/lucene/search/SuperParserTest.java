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
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

public class SuperParserTest {

  private SuperParser parser;
  private BlurAnalyzer analyzer;

  @Before
  public void setup() {
    AnalyzerDefinition ad = new AnalyzerDefinition();
    ad.setDefaultDefinition(new ColumnDefinition(StandardAnalyzer.class.getName(), true, null));
    ColumnFamilyDefinition cfDef = new ColumnFamilyDefinition();
    cfDef.putToColumnDefinitions("id_l", new ColumnDefinition("long", false, null));
    cfDef.putToColumnDefinitions("id_d", new ColumnDefinition("double", false, null));
    cfDef.putToColumnDefinitions("id_f", new ColumnDefinition("float", false, null));
    cfDef.putToColumnDefinitions("id_i", new ColumnDefinition("integer", false, null));
    ad.putToColumnFamilyDefinitions("a", cfDef);
    analyzer = new BlurAnalyzer(ad);
    parser = new SuperParser(LUCENE_VERSION, new BlurAnalyzer(new WhitespaceAnalyzer(LUCENE_VERSION)), true, null,
        ScoreType.SUPER, new Term("_primedoc_"));
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
    SuperParser parser = new SuperParser(LUCENE_VERSION, analyzer, true, null, ScoreType.SUPER, new Term("_primedoc_"));
    try {
      parser.parse("super : <a:a d:{e TO d} b:b super:<test:hello\\<>> super:<c:c d:d>");
      fail();
    } catch (ParseException e) {
      // should throw an error
    }
  }

  @Test
  public void test7() throws ParseException {
    Query q = parseSq("(a.b:cool) (+a.c:cool a.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc_m(tq("a.c", "cool")), bc(tq("a.b", "cool"))))), q);
  }

  @Test
  public void test8() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool a.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool"))))), q);
  }

  @Test
  public void test9() throws ParseException {
    Query q = parseSq("a.b:cool (a.c:cool a.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool"))))), q);
  }

  @Test
  public void test10() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool a.b:cool");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(tq("a.c", "cool")), bc(tq("a.b", "cool"))), q);
  }

  @Test
  public void test11() throws ParseException {
    Query q = parseSq("(a.b:cool) (+a.c:cool c.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc_m(tq("a.c", "cool")), bc(tq("c.b", "cool"))))), q);
  }

  @Test
  public void test12() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool c.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool"))))), q);
  }

  @Test
  public void test13() throws ParseException {
    Query q = parseSq("a.b:cool (a.c:cool c.b:cool)");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool"))))), q);
  }

  @Test
  public void test14() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool c.b:cool");
    assertQuery(bq(bc(tq("a.b", "cool")), bc(tq("a.c", "cool")), bc(tq("c.b", "cool"))), q);
  }

  @Test
  public void test15() throws ParseException {
    Query q = parseSq("a.id_l:[0 TO 2]");
    Query q1 = rq_i("a.id_l", 0L, 2L);
    assertQuery(q1, q);
  }

  @Test
  public void test16() throws ParseException {
    Query q = parseSq("a.id_d:[0 TO 2]");
    assertQuery(rq_i("a.id_d", 0.0D, 2.0D), q);
  }

  @Test
  public void test17() throws ParseException {
    Query q = parseSq("a.id_f:[0 TO 2]");
    assertQuery(rq_i("a.id_f", 0.0F, 2.0F), q);
  }

  @Test
  public void test18() throws ParseException {
    Query q = parseSq("a.id_i:[0 TO 2]");
    Query q1 = rq_i("a.id_i", 0, 2);
    assertQuery(q1, q);
  }

  public static BooleanClause bc_m(Query q) {
    return new BooleanClause(q, Occur.MUST);
  }

  public static BooleanClause bc_n(Query q) {
    return new BooleanClause(q, Occur.MUST_NOT);
  }

  public static BooleanClause bc(Query q) {
    return new BooleanClause(q, Occur.SHOULD);
  }

  public static void assertQuery(Query expected, Query actual) {
    System.out.println(expected);
    System.out.println(actual);
    assertEqualsQuery(expected, actual);
  }

  public static void assertEqualsQuery(Query expected, Query actual) {
    assertEquals(expected.getClass(), actual.getClass());
    if (expected instanceof BooleanQuery) {
      assertEqualsBooleanQuery((BooleanQuery) expected, (BooleanQuery) actual);
    } else if (expected instanceof SuperQuery) {
      assertEqualsSuperQuery((SuperQuery) expected, (SuperQuery) actual);
    } else if (expected instanceof TermQuery) {
      assertEqualsTermQuery((TermQuery) expected, (TermQuery) actual);
    } else if (expected instanceof NumericRangeQuery<?>) {
      assertEqualsNumericRangeQuery((NumericRangeQuery<?>) expected, (NumericRangeQuery<?>) actual);
    } else {
      fail("Type [" + expected.getClass() + "] not supported");
    }
  }

  public static void assertEqualsTermQuery(TermQuery expected, TermQuery actual) {
    Term term1 = expected.getTerm();
    Term term2 = actual.getTerm();
    assertEquals(term1, term2);
  }

  public static void assertEqualsNumericRangeQuery(NumericRangeQuery<?> expected, NumericRangeQuery<?> actual) {
    assertEquals(expected, actual);
  }

  public static void assertEqualsSuperQuery(SuperQuery expected, SuperQuery actual) {
    assertEquals(expected.getQuery(), actual.getQuery());
  }

  public static void assertEqualsBooleanQuery(BooleanQuery expected, BooleanQuery actual) {
    List<BooleanClause> clauses1 = expected.clauses();
    List<BooleanClause> clauses2 = actual.clauses();
    assertEqualsBooleanClause(clauses1, clauses2);
  }

  public static void assertEqualsBooleanClause(List<BooleanClause> clauses1, List<BooleanClause> clauses2) {
    if (clauses1 == null && clauses2 == null) {
      return;
    }
    if (clauses1 == null || clauses2 == null) {
      fail();
    }
    if (clauses1.size() != clauses2.size()) {
      fail();
    }
    int size = clauses1.size();
    for (int i = 0; i < size; i++) {
      assertEqualsBooleanClause(clauses1.get(i), clauses2.get(i));
    }
  }

  public static void assertEqualsBooleanClause(BooleanClause booleanClause1, BooleanClause booleanClause2) {
    assertEquals(booleanClause1.getOccur(), booleanClause2.getOccur());
    assertEqualsQuery(booleanClause1.getQuery(), booleanClause2.getQuery());
  }

  private Query rq_i(String field, float min, float max) {
    return NumericRangeQuery.newFloatRange(field, min, max, true, true);
  }

  private Query rq_i(String field, int min, int max) {
    return NumericRangeQuery.newIntRange(field, min, max, true, true);
  }

  private Query rq_i(String field, double min, double max) {
    return NumericRangeQuery.newDoubleRange(field, min, max, true, true);
  }

  private Query rq_i(String field, long min, long max) {
    return NumericRangeQuery.newLongRange(field, min, max, true, true);
  }

  private BooleanQuery bq(BooleanClause... bcs) {
    BooleanQuery bq = new BooleanQuery();
    for (BooleanClause bc : bcs) {
      bq.add(bc);
    }
    return bq;
  }

  private SuperQuery sq(Query q) {
    return new SuperQuery(q, ScoreType.SUPER, new Term("_primedoc_"));
  }

  private TermQuery tq(String field, String text) {
    return new TermQuery(new Term(field, text));
  }

  private Query parseSq(String qstr) throws ParseException {
    SuperParser superParser = new SuperParser(LUCENE_VERSION, analyzer, true, null, ScoreType.SUPER, new Term(
        "_primedoc_"));
    return superParser.parse(qstr);
  }

}
