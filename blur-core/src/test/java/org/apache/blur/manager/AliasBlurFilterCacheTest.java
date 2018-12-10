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
package org.apache.blur.manager;

import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.lucene.search.SuperParser;
import org.apache.blur.manager.BlurFilterCache.FilterParser;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.junit.Test;

public class AliasBlurFilterCacheTest {

  private static final String TABLE = "test";
  private static final String TEST_FILTER = "(abc1 abc2 abc3)";

  @Test
  public void testFetchPreFilterEmpty() throws IOException {
    BlurConfiguration configuration = new BlurConfiguration();
    AliasBlurFilterCache defaultBlurFilterCache = new AliasBlurFilterCache(configuration);
    Filter fetchPreFilter = defaultBlurFilterCache.fetchPreFilter(TABLE, TEST_FILTER);
    assertNull(fetchPreFilter);
  }

  @Test
  public void testFetchPreFilterNotEmpty() throws IOException, BlurException, ParseException {
    BlurConfiguration configuration = new BlurConfiguration();
    configuration.set("blur.filter.alias.test.super:abc1", "(fam1.f1:abc1 fam2.f1:abc1)");
    configuration.set("blur.filter.alias.test.super:abc2", "(fam1.f1:abc2 fam2.f1:abc2)");
    configuration.set("blur.filter.alias.test.super:abc3", "(fam1.f1:abc3 fam2.f1:abc3)");
    AliasBlurFilterCache defaultBlurFilterCache = new AliasBlurFilterCache(configuration);

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TABLE);
    tableDescriptor.setTableUri("file:///");
    final TableContext tableContext = TableContext.create(tableDescriptor);

    final BaseFieldManager fieldManager = getFieldManager(new WhitespaceAnalyzer(LUCENE_VERSION));

    Filter filter = QueryParserUtil.parseFilter(TABLE, TEST_FILTER, false, fieldManager, defaultBlurFilterCache,
        tableContext);
    Filter filterToRun = defaultBlurFilterCache.storePreFilter(TABLE, TEST_FILTER, filter, new FilterParser() {
      @Override
      public Query parse(String query) throws ParseException {
        return new SuperParser(LUCENE_VERSION, fieldManager, false, null, ScoreType.CONSTANT, tableContext
            .getDefaultPrimeDocTerm()).parse(query);
      }
    });
    assertNotNull(filterToRun);
    assertEquals("BooleanFilter(" + "FilterCache(super-abc1,QueryWrapperFilter(fam1.f1:abc1 fam2.f1:abc1)) "
        + "FilterCache(super-abc2,QueryWrapperFilter(fam1.f1:abc2 fam2.f1:abc2)) "
        + "FilterCache(super-abc3,QueryWrapperFilter(fam1.f1:abc3 fam2.f1:abc3))" + ")", filterToRun.toString());

    Filter fetchPreFilter = defaultBlurFilterCache.fetchPreFilter(TABLE, TEST_FILTER);
    assertNotNull(fetchPreFilter);

    assertTrue(filterToRun == fetchPreFilter);

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

    fieldManager.addColumnDefinition(null, "bin", null, false, "string", false, false, null);
    fieldManager.addColumnDefinitionInt("a", "id_i");
    fieldManager.addColumnDefinitionDouble("a", "id_d");
    fieldManager.addColumnDefinitionFloat("a", "id_f");
    fieldManager.addColumnDefinitionLong("a", "id_l");
    fieldManager.addColumnDefinitionDate("a", "id_date", "yyyy-MM-dd");
    fieldManager.addColumnDefinitionGisRecursivePrefixTree("a", "id_gis");
    return fieldManager;
  }
}
