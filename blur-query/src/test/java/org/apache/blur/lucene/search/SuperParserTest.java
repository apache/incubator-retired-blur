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
package org.apache.blur.lucene.search;

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.analysis.type.spatial.ShapeReadWriter;
import org.apache.blur.analysis.type.spatial.SpatialArgsParser;
import org.apache.blur.analysis.type.spatial.lucene.RecursivePrefixTreeStrategy;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;

public class SuperParserTest {

  private SuperParser parser;
  private BaseFieldManager _fieldManager;

  @Before
  public void setup() throws IOException {
    _fieldManager = getFieldManager(new NoStopWordStandardAnalyzer());
    parser = new SuperParser(LUCENE_VERSION, _fieldManager, true, null, ScoreType.SUPER, new Term("_primedoc_"));
  }

  private BaseFieldManager getFieldManager(Analyzer a) throws IOException {
    BaseFieldManager fieldManager = new BaseFieldManager(BlurConstants.SUPER, a, new Configuration()) {
      @Override
      protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) {
        return true;
      }

      @Override
      protected void tryToLoad(String fieldName) {

      }

      @Override
      protected List<String> getFieldNamesToLoad() throws IOException {
        return new ArrayList<String>();
      }
    };

    fieldManager.addColumnDefinition(null, "bin", null, false, "string", true, false, null);
    fieldManager.addColumnDefinitionInt("a", "id_i");
    fieldManager.addColumnDefinitionDouble("a", "id_d");
    fieldManager.addColumnDefinitionFloat("a", "id_f");
    fieldManager.addColumnDefinitionLong("a", "id_l");
    fieldManager.addColumnDefinitionDate("a", "id_date", "yyyy-MM-dd");
    fieldManager.addColumnDefinitionGisRecursivePrefixTree("a", "id_gis");
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

  @Test
  public void test28() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    ShapeReadWriter<SpatialContext> shapeReadWriter = new ShapeReadWriter<SpatialContext>(ctx);
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_KM));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    String writeSpatialArgs = SpatialArgsParser.writeSpatialArgs(args, shapeReadWriter);

    // This has to be done because of rounding.
    SpatialArgs spatialArgs = SpatialArgsParser.parse(writeSpatialArgs, shapeReadWriter);
    Query q1 = sq(strategy.makeQuery(spatialArgs));
    Query q = parseSq("a.id_gis:\"" + writeSpatialArgs + "\"");
    boolean equals = q1.equals(q);
    assertTrue(equals);
  }

  @Test
  public void test29() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_KM));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    Query q1 = sq(strategy.makeQuery(args));
    Query q = parseSq("a.id_gis:\"Intersects(Circle(33.000000,-80.000000 d=10.0km))\"");
    boolean equals = q1.equals(q);
    assertTrue(equals);
  }

  @Test
  public void test30() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_MI));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    Query q1 = sq(strategy.makeQuery(args));
    Query q = parseSq("a.id_gis:\"Intersects(Circle(33.000000,-80.000000 d=10.0m))\"");
    boolean equals = q1.equals(q);
    assertTrue(equals);
  }

  @Test
  public void test31() throws ParseException, java.text.ParseException {
    Query q = parseSq("a.id_date:2013-09-13");
    Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2013-09-13");
    long time = date.getTime();
    long converted = TimeUnit.SECONDS.convert(time, TimeUnit.MILLISECONDS);
    Query q1 = sq(rq_i("a.id_date", converted, converted));
    assertQuery(q1, q);
  }

  @Test
  public void test32() throws ParseException, java.text.ParseException {
    Query q = parseSq("a.id_date:[2013-09-13 TO 2013-11-13]");
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date d1 = simpleDateFormat.parse("2013-09-13");
    Date d2 = simpleDateFormat.parse("2013-11-13");
    long t1 = TimeUnit.SECONDS.convert(d1.getTime(), TimeUnit.MILLISECONDS);
    long t2 = TimeUnit.SECONDS.convert(d2.getTime(), TimeUnit.MILLISECONDS);
    Query q1 = sq(rq_i("a.id_date", t1, t2));
    assertQuery(q1, q);
  }

  @Test
  public void test33() throws ParseException, java.text.ParseException {
    Query q = parseSq("a.id_date:{2013-09-13 TO 2013-11-13}");
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date d1 = simpleDateFormat.parse("2013-09-13");
    Date d2 = simpleDateFormat.parse("2013-11-13");
    long t1 = TimeUnit.SECONDS.convert(d1.getTime(), TimeUnit.MILLISECONDS);
    long t2 = TimeUnit.SECONDS.convert(d2.getTime(), TimeUnit.MILLISECONDS);
    Query q1 = sq(rq_e("a.id_date", t1, t2));
    assertQuery(q1, q);
  }

  @Test
  public void test34() throws ParseException {
    Query q = parseSq("rowid:123-456");
    Query q1 = sq(tq("rowid", "123-456"));
    assertQuery(q1, q);
  }

  @Test
  public void test35() throws ParseException {
    Query q = parseSq("family:123-456");
    Query q1 = sq(tq("family", "123-456"));
    assertQuery(q1, q);
  }

  @Test
  public void test36() throws ParseException {
    Query q = parseSq("recordid:123-456");
    Query q1 = sq(tq("recordid", "123-456"));
    assertQuery(q1, q);
  }

  @Test
  public void test37() throws ParseException {
    Query q = parseSq("bin:cool");
    Query q1 = sq(tq("_default_.bin", "cool"));
    assertQuery(q1, q);
  }

  @Test
  public void test38() throws ParseException {
    Query q = parseSq("-<f.c:a> -<f.c:b>");
    Query q1 = sq(tq("f.c", "a"));
    Query q2 = sq(tq("f.c", "b"));
    Query q3 = new TermQuery(new Term("_primedoc_", "true"));
    BooleanQuery bq = bq(bc_n(q1), bc_n(q2), bc(q3));
    assertQuery(bq, q);
  }

  @Test
  public void test39() throws ParseException {
    Query q = parseSq("<-f.c:a> <-f.c:b>");
    Query q1 = sq(bq(bc_n(tq("f.c", "a")), bc(new MatchAllDocsQuery())));
    Query q2 = sq(bq(bc_n(tq("f.c", "b")), bc(new MatchAllDocsQuery())));
    BooleanQuery bq = bq(bc(q1), bc(q2));
    assertQuery(bq, q);
  }

  @Test
  public void test40() throws ParseException {
    Query q = parseSq("-<-f.c:a> -<-f.c:b>");
    Query q1 = sq(bq(bc_n(tq("f.c", "a")), bc(new MatchAllDocsQuery())));
    Query q2 = sq(bq(bc_n(tq("f.c", "b")), bc(new MatchAllDocsQuery())));
    Query q3 = new TermQuery(new Term("_primedoc_", "true"));
    BooleanQuery bq = bq(bc_n(q1), bc_n(q2), bc(q3));
    assertQuery(bq, q);
  }

  @Test
  public void test41() throws ParseException {
    Query q = parseSq("<*>");
    Query q1 = sq(new MatchAllDocsQuery());
    assertQuery(q1, q);
  }

  @Test
  public void test42() throws ParseException {
    Query q = parseSq("<f.c:*abc>");
    Query q1 = sq(new WildcardQuery(new Term("f.c", "*abc")));
    assertQuery(q1, q);
  }

  @Test
  public void test43() throws ParseException {
    Query q1 = parseSq("+(+f.c:(s\\-a\\-b))");
    Query q2 = parseSq("+<+f.c:(s\\-a\\-b)>");
    assertQuery(q1, q2);
  }

  @Test
  public void test44() throws ParseException {
    Query q1 = parseSq("<f.c:abc recordid:123>");
    Query q2 = sq(bq(bc(tq("f.c", "abc")), bc(new TermQuery(new Term("recordid", "123")))));
    assertQuery(q1, q2);
  }

  @Test
  public void test45() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_MI));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    Query q1 = sq(bq(bc_m(strategy.makeQuery(args))));
    Query q = parseSq("<+a.id_gis:\"Intersects(Circle(33.000000,-80.000000 d=10.0m))\">");
    boolean equals = q1.equals(q);
    assertTrue(equals);
  }

  @Test
  public void test46() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_MI));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    Query q1 = sq(bq(bc_m(strategy.makeQuery(args)), bc(tq("rowid", "12345"))));
    Query q = parseSq("<+a.id_gis:\"Intersects(Circle(33.000000,-80.000000 d=10.0m))\" rowid:12345>");
    boolean equals = q1.equals(q);
    assertTrue(equals);
  }

  @Test
  public void test47() throws ParseException {
    SpatialContext ctx = SpatialContext.GEO;
    int maxLevels = 11;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "a.id_gis", false);
    Circle circle = ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(10, DistanceUtils.EARTH_MEAN_RADIUS_MI));
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);

    Query q1 = sq(strategy.makeQuery(args));
    Query q = parseSq("<a.id_gis:\"Intersects(Circle(33.000000,-80.000000 d=10.0m))\">");
    boolean equals = q1.equals(q);
    assertTrue(equals);
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

  private Query rq_e(String field, long min, long max) {
    return NumericRangeQuery.newLongRange(field, min, max, false, false);
  }

  private BooleanQuery bq(BooleanClause... bcs) {
    BooleanQuery bq = new BooleanQuery();
    for (BooleanClause bc : bcs) {
      bq.add(bc);
    }
    return bq;
  }

  private SuperQuery sq(Query q) {
    return new SuperQuery(q, ScoreType.SUPER, new Term("_primedoc_", "true"));
  }

  private TermQuery tq(String field, String text) {
    return new TermQuery(new Term(field, text));
  }

  private Query parseSq(String qstr) throws ParseException {
    SuperParser superParser = new SuperParser(LUCENE_VERSION, _fieldManager, true, null, ScoreType.SUPER, new Term(
        "_primedoc_", "true"));
    return superParser.parse(qstr);
  }

}
