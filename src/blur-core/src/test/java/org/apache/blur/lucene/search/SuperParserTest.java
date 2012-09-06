package org.apache.blur.lucene.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class SuperParserTest {

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
  }

  @Test
  public void test1() throws ParseException {
    Query q = parseSq("(a.b:cool) (+a.c:cool a.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc_m(tq("a.c", "cool")), bc(tq("a.b", "cool")))))), q);
  }

  @Test
  public void test2() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool a.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool")))))), q);
  }

  @Test
  public void test3() throws ParseException {
    Query q = parseSq("a.b:cool (a.c:cool a.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool")))))), q);
  }

  @Test
  public void test4() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool a.b:cool");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(tq("a.c", "cool"))), bc(sq(tq("a.b", "cool")))), q);
  }

  @Test
  public void test5() throws ParseException {
    Query q = parseSq("(a.b:cool) (+a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc_m(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test6() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test7() throws ParseException {
    Query q = parseSq("a.b:cool (a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test8() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool c.b:cool");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(tq("a.c", "cool"))), bc(sq(tq("c.b", "cool")))), q);
  }

  @Test
  public void test9() throws ParseException {
    Query q = parseSq("a.id_l:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_l", 0L, 2L)), q);
  }
  
  @Test
  public void test10() throws ParseException {
    Query q = parseSq("a.id_d:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_d", 0.0D, 2.0D)), q);
  }
  
  @Test
  public void test11() throws ParseException {
    Query q = parseSq("a.id_f:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_f", 0.0F, 2.0F)), q);
  }
  
  @Test
  public void test12() throws ParseException {
    Query q = parseSq("a.id_i:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_i", 0, 2)), q);
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
    } else {
      fail("Type [" + expected.getClass() + "] not supported");
    }
  }

  public static void assertEqualsTermQuery(TermQuery expected, TermQuery actual) {
    Term term1 = expected.getTerm();
    Term term2 = actual.getTerm();
    assertEquals(term1, term2);
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
    return new SuperQuery(q, ScoreType.SUPER);
  }

  private TermQuery tq(String field, String text) {
    return new TermQuery(new Term(field, text));
  }

  private Query parseSq(String qstr) throws ParseException {
    SuperParser parser = new SuperParser(Version.LUCENE_36, analyzer, true, null, ScoreType.SUPER);
    return parser.parse(qstr);
  }
}
