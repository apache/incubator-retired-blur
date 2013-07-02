package org.apache.blur.analysis;

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


public class BlurAnalyzerTest {

//  private static final String STANDARD = "org.apache.lucene.analysis.standard.StandardAnalyzer";
//
//  @Test
//  public void testToAndFromJSONDef1() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef1());
//    String json = analyzer.toJSON();
//    BlurAnalyzer analyzer2 = BlurAnalyzer.create(json);
//    assertEquals(analyzer.getAnalyzerDefinition(), analyzer2.getAnalyzerDefinition());
//  }
//
//  @Test
//  public void testStoringOfFieldDef1() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef1());
//    assertFalse(analyzer.getFieldType("b.c.sub1").stored());
//    assertTrue(analyzer.getFieldType("b.c").stored());
//  }
//
//  @Test
//  public void testGetSubFieldsDef1() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef1());
//    assertNull(analyzer.getSubIndexNames("b.d"));
//    Set<String> subIndexNames = analyzer.getSubIndexNames("b.c");
//    TreeSet<String> set = new TreeSet<String>();
//    set.add("b.c.sub1");
//    set.add("b.c.sub2");
//    assertEquals(set, subIndexNames);
//  }
//
//  @Test
//  public void testFullTextFieldsDef1() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef1());
//    assertTrue(analyzer.isFullTextField("a.b"));
//    assertFalse(analyzer.isFullTextField("a.d"));
//  }
//
//  @Test
//  public void testToAndFromJSONDef2() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef2());
//    String json = analyzer.toJSON();
//    BlurAnalyzer analyzer2 = BlurAnalyzer.create(json);
//    assertEquals(analyzer.getAnalyzerDefinition(), analyzer2.getAnalyzerDefinition());
//  }
//
//  @Test
//  public void testStoringOfFieldDef2() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef2());
//    assertTrue(analyzer.getFieldType("a.b").stored());
//    assertTrue(analyzer.getFieldType("b.c").stored());
//  }
//
//  @Test
//  public void testGetSubFieldsDef2() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef2());
//    assertNull(analyzer.getSubIndexNames("b.d"));
//  }
//
//  @Test
//  public void testFullTextFieldsDef2() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer(getDef2());
//    assertTrue(analyzer.isFullTextField("a.b"));
//    assertFalse(analyzer.isFullTextField("d.a"));
//  }
//  
//  @Test
//  public void testFullTextFieldsDefault() throws IOException {
//    BlurAnalyzer analyzer = new BlurAnalyzer();
//    assertTrue(analyzer.isFullTextField("a.b"));
//    assertTrue(analyzer.isFullTextField("d.a"));
//  }
//
//  private AnalyzerDefinition getDef1() {
//
//    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition().setDefaultDefinition(
//        new ColumnDefinition(STANDARD, false, null)).setFullTextAnalyzerClassName(STANDARD);
//    Map<String, ColumnFamilyDefinition> colFamDefs = new HashMap<String, ColumnFamilyDefinition>();
//
//    ColumnFamilyDefinition aColFamDef;
//    Map<String, ColumnDefinition> aColDef;
//    Map<String, ColumnDefinition> bColDef;
//    Map<String, ColumnDefinition> cColDef;
//    Map<String, AlternateColumnDefinition> alternates;
//    ColumnFamilyDefinition bColFamDef;
//    ColumnFamilyDefinition cColFamDef;
//    
//    aColFamDef = new ColumnFamilyDefinition();
//    
//    aColDef = new HashMap<String, ColumnDefinition>();
//    aColDef.put("b", new ColumnDefinition(STANDARD, true, null));
//    aColFamDef.setColumnDefinitions(aColDef);
//    colFamDefs.put("a", aColFamDef);
//    
//    bColDef = new HashMap<String, ColumnDefinition>();
//
//    alternates = new HashMap<String, AlternateColumnDefinition>();
//    alternates.put("sub1", new AlternateColumnDefinition(STANDARD));
//    alternates.put("sub2", new AlternateColumnDefinition(STANDARD));
//    bColDef.put("c", new ColumnDefinition(STANDARD, true, alternates));
//
//    bColFamDef = new ColumnFamilyDefinition();
//    bColFamDef.setColumnDefinitions(bColDef);
//    colFamDefs.put("b", bColFamDef);
//    
//    cColFamDef = new ColumnFamilyDefinition();
//    cColDef = new HashMap<String, ColumnDefinition>();
//    cColDef.put("cint", new ColumnDefinition(BlurAnalyzer.TYPE.INTEGER.name(), true, null));
//    cColFamDef.setColumnDefinitions(cColDef);
//    
//    colFamDefs.put("c", cColFamDef);
//
//    analyzerDefinition.setColumnFamilyDefinitions(colFamDefs);
//    return analyzerDefinition;
//  }
//
//  private AnalyzerDefinition getDef2() {
//    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition().setDefaultDefinition(
//        new ColumnDefinition(STANDARD, false, null)).setFullTextAnalyzerClassName(STANDARD);
//    analyzerDefinition.putToColumnFamilyDefinitions("a",
//        new ColumnFamilyDefinition().setDefaultDefinition(new ColumnDefinition(STANDARD, true, null)));
//    return analyzerDefinition;
//  }
}
