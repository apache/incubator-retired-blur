package org.apache.blur.lucene.search;

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
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
  private BaseFieldManager _fieldManager;

  @Before
  public void setup() throws IOException {
    // AnalyzerDefinition ad = new AnalyzerDefinition();
    // ad.setDefaultDefinition(new
    // ColumnDefinition(NoStopWordStandardAnalyzer.class.getName(), true,
    // null));
    // ColumnFamilyDefinition cfDef = new ColumnFamilyDefinition();
    // cfDef.putToColumnDefinitions("id_l", new ColumnDefinition("long", false,
    // null));
    // cfDef.putToColumnDefinitions("id_d", new ColumnDefinition("double",
    // false, null));
    // cfDef.putToColumnDefinitions("id_f", new ColumnDefinition("float", false,
    // null));
    // cfDef.putToColumnDefinitions("id_i", new ColumnDefinition("integer",
    // false, null));
    // ad.putToColumnFamilyDefinitions("a", cfDef);

    _fieldManager = getFieldManager(new NoStopWordStandardAnalyzer());
    parser = new SuperParser(LUCENE_VERSION, _fieldManager, true, null, ScoreType.SUPER, new Term("_primedoc_"));
  }

  private BaseFieldManager getFieldManager(Analyzer a) throws IOException {
    BaseFieldManager fieldManager = new BaseFieldManager(BlurConstants.SUPER, a) {
      @Override
      protected boolean tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType,
          Map<String, String> props) {
        return true;
      }

      @Override
      protected void tryToLoad(String fieldName) {

      }
    };

    fieldManager.addColumnDefinitionInt("a", "id_i");
    fieldManager.addColumnDefinitionDouble("a", "id_d");
    fieldManager.addColumnDefinitionFloat("a", "id_f");
    fieldManager.addColumnDefinitionLong("a", "id_l");
    return fieldManager;
  }

  @Test
  public void test1() throws ParseException {
    Query query = parser.parse(" +<a.a:a a.d:e a.b:b> ");

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term("a.a", "a")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("a.d", "e")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("a.b", "b")), Occur.SHOULD);
    SuperQuery superQuery = new SuperQuery(booleanQuery, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery bq = new BooleanQuery();
    bq.add(superQuery, Occur.MUST);

    assertQuery(bq, query);

  }

  @Test
  public void test2() throws ParseException {
    Query query = parser.parse("<a.c:c a.d:d>");

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term("a.c", "c")), Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term("a.d", "d")), Occur.SHOULD);
    SuperQuery superQuery = new SuperQuery(booleanQuery, ScoreType.SUPER, new Term("_primedoc_"));

    assertQuery(superQuery, query);
  }

  @Test
  public void test3() throws ParseException {
    Query query = parser.parse("a:a d:e b:b");
    assertQuery(bq(bc(sq(tq("a", "a"))), bc(sq(tq("d", "e"))), bc(sq(tq("b", "b")))), query);
  }

  @Test
  public void test4() throws ParseException {
    Query query = parser.parse("<a.a:a a.d:e a.b:b>  -<b.c:c b.d:d>");

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term("a.a", "a")), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("a.d", "e")), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("a.b", "b")), Occur.SHOULD);

    BooleanQuery booleanQuery2 = new BooleanQuery();
    booleanQuery2.add(new TermQuery(new Term("b.c", "c")), Occur.SHOULD);
    booleanQuery2.add(new TermQuery(new Term("b.d", "d")), Occur.SHOULD);

    SuperQuery superQuery1 = new SuperQuery(booleanQuery1, ScoreType.SUPER, new Term("_primedoc_"));
    SuperQuery superQuery2 = new SuperQuery(booleanQuery2, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(superQuery1, Occur.SHOULD);
    booleanQuery.add(superQuery2, Occur.MUST_NOT);

    assertQuery(booleanQuery, query);
  }

  @Test
  public void test5() throws ParseException, IOException {
    parser = new SuperParser(LUCENE_VERSION, getFieldManager(new WhitespaceAnalyzer(LUCENE_VERSION)), true, null,
        ScoreType.SUPER, new Term("_primedoc_"));
    Query query = parser.parse("<a.a:a a.d:{e TO f} a.b:b a.test:hello\\<> -<g.c:c g.d:d>");

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term("a.a", "a")), Occur.SHOULD);
    booleanQuery1.add(new TermRangeQuery("a.d", new BytesRef("e"), new BytesRef("f"), false, false), Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term("a.b", "b")), Occur.SHOULD);
    // std analyzer took the "<" out
    booleanQuery1.add(new TermQuery(new Term("a.test", "hello<")), Occur.SHOULD);

    BooleanQuery booleanQuery2 = new BooleanQuery();
    booleanQuery2.add(new TermQuery(new Term("g.c", "c")), Occur.SHOULD);
    booleanQuery2.add(new TermQuery(new Term("g.d", "d")), Occur.SHOULD);

    SuperQuery superQuery1 = new SuperQuery(booleanQuery1, ScoreType.SUPER, new Term("_primedoc_"));
    SuperQuery superQuery2 = new SuperQuery(booleanQuery2, ScoreType.SUPER, new Term("_primedoc_"));

    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(superQuery1, Occur.SHOULD);
    booleanQuery.add(superQuery2, Occur.MUST_NOT);

    assertQuery(booleanQuery, query);
  }

  @Test
  public void test6() throws ParseException {
    // analyzer
    SuperParser parser = new SuperParser(LUCENE_VERSION, _fieldManager, true, null, ScoreType.SUPER, new Term(
        "_primedoc_"));
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
    BooleanQuery bq = bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc_m(tq("a.c", "cool")), bc(tq("a.b", "cool"))))));

    System.out.println(q);
    System.out.println(bq);
    assertQuery(bq, q);
  }

  @Test
  public void test8() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool a.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool")))))), q);
  }

  @Test
  public void test9() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool a.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("a.b", "cool")))))), q);
  }

  @Test
  public void test10() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool a.b:cool");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(tq("a.c", "cool"))), bc(sq(tq("a.b", "cool")))), q);
  }

  @Test
  public void test11() throws ParseException {
    Query q = parseSq("(a.b:cool) (+a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc_m(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test12() throws ParseException {
    Query q = parseSq("(a.b:cool) (a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test13() throws ParseException {
    Query q = parseSq("a.b:cool (a.c:cool c.b:cool)");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(bq(bc(tq("a.c", "cool")), bc(tq("c.b", "cool")))))), q);
  }

  @Test
  public void test14() throws ParseException {
    Query q = parseSq("a.b:cool a.c:cool c.b:cool");
    assertQuery(bq(bc(sq(tq("a.b", "cool"))), bc(sq(tq("a.c", "cool"))), bc(sq(tq("c.b", "cool")))), q);
  }

  @Test
  public void test15() throws ParseException {
    Query q = parseSq("a.id_l:[0 TO 2]");
    Query q1 = rq_i("a.id_l", 0L, 2L);
    assertQuery(sq(q1), q);
  }

  @Test
  public void test16() throws ParseException {
    Query q = parseSq("a.id_d:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_d", 0.0D, 2.0D)), q);
  }

  @Test
  public void test17() throws ParseException {
    Query q = parseSq("a.id_f:[0 TO 2]");
    assertQuery(sq(rq_i("a.id_f", 0.0F, 2.0F)), q);
  }

  @Test
  public void test18() throws ParseException {
    Query q = parseSq("a.id_i:[0 TO 2]");
    Query q1 = rq_i("a.id_i", 0, 2);
    assertQuery(sq(q1), q);
  }

  @Test
  public void test19() throws ParseException {
    Query q = parseSq("word1");
    Query q1 = sq(tq("super", "word1"));
    assertQuery(q1, q);
  }

  @Test
  public void test20() throws ParseException {
    Query q = parseSq("word1 word2");
    Query q1 = bq(bc(sq(tq("super", "word1"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test21() throws ParseException {
    Query q = parseSq("<f1:word1> word2");
    Query q1 = bq(bc(sq(tq("f1", "word1"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test22() throws ParseException {
    Query q = parseSq("<f1:word1> word2 <word3>");
    Query q1 = bq(bc(sq(tq("f1", "word1"))), bc(sq(tq("super", "word2"))), bc(sq(tq("super", "word3"))));
    assertQuery(q1, q);
  }

  @Test
  public void test23() throws ParseException {
    Query q = parseSq("<f1:word1>  <word3> word2");
    Query q1 = bq(bc(sq(tq("f1", "word1"))), bc(sq(tq("super", "word3"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test24() throws ParseException {
    Query q = parseSq("<f1:word1> +word6 <word3> word2");
    Query q1 = bq(bc(sq(tq("f1", "word1"))), bc_m(sq(tq("super", "word6"))), bc(sq(tq("super", "word3"))),
        bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test25() throws ParseException {
    Query q = parseSq("+leading <f1:word1> +word6 <word3> word2");
    Query q1 = bq(bc_m(sq(tq("super", "leading"))), bc(sq(tq("f1", "word1"))), bc_m(sq(tq("super", "word6"))),
        bc(sq(tq("super", "word3"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test25_AND_ORs() throws ParseException {
    Query q = parseSq("leading AND <f1:word1> OR word6 <word3> word2");
    Query q1 = bq(bc_m(sq(tq("super", "leading"))), bc_m(sq(tq("f1", "word1"))), bc(sq(tq("super", "word6"))),
        bc(sq(tq("super", "word3"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test26() throws ParseException {
    Query q = parseSq("-leading <f1:word1> +word6 <word3> word2");
    Query q1 = bq(bc_n(sq(tq("super", "leading"))), bc(sq(tq("f1", "word1"))), bc_m(sq(tq("super", "word6"))),
        bc(sq(tq("super", "word3"))), bc(sq(tq("super", "word2"))));
    assertQuery(q1, q);
  }

  @Test
  public void test27() throws ParseException {
    Query q = parseSq("rowid:1");
    Query q1 = sq(tq("rowid", "1"));
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
    System.out.println("expected =" + expected);
    System.out.println("actual   =" + actual);
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
    SuperParser superParser = new SuperParser(LUCENE_VERSION, _fieldManager, true, null, ScoreType.SUPER, new Term(
        "_primedoc_"));
    return superParser.parse(qstr);
  }

}
