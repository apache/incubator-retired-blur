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
package org.apache.blur.analysis.type.spatial;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.lucene.search.SuperParser;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class SpatialTermQueryPrefixTreeStrategyFieldTypeDefinitionGeohashTest extends
    BaseSpatialFieldTypeDefinitionTest {

  @Test
  public void testTermQueryPrefixTree() throws IOException, ParseException {
    runGisTypeTest();
    testGeoHash();
  }

  public void testGeoHash() throws IOException, ParseException {
    BaseFieldManager fieldManager = getFieldManager(new NoStopWordStandardAnalyzer());
    setupGisField(fieldManager);
    DirectoryReader reader = DirectoryReader.open(_dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    SuperParser parser = new SuperParser(Version.LUCENE_43, fieldManager, true, null, ScoreType.SUPER, new Term(
        BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE));

    Query query = parser.parse("fam.geo:\"GeoHash(uvgb26kqsm0)\"");

    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits);

    reader.close();
  }

  protected void setupGisField(BaseFieldManager fieldManager) throws IOException {
    Map<String, String> props = new HashMap<String, String>();
    props.put(BaseSpatialFieldTypeDefinition.SPATIAL_PREFIX_TREE, BaseSpatialFieldTypeDefinition.GEOHASH_PREFIX_TREE);
    fieldManager.addColumnDefinition("fam", "geo", null, false,
        SpatialTermQueryPrefixTreeStrategyFieldTypeDefinition.NAME, false, false, props);
  }

}
