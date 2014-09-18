package org.apache.blur.analysis.type.spatial;

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
import java.util.Map;

import org.apache.blur.analysis.type.spatial.lucene.RecursivePrefixTreeStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.util.BytesRef;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

public class SpatialRecursivePrefixTreeStrategyFieldTypeDefinition extends BaseSpatialFieldTypeDefinition {

  public static final String NAME = "geo-recursiveprefix";
  public static final String DOC_VALUE = "docValue";
  private static final Analyzer _keyword = new KeywordAnalyzer();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    return _keyword;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    _ctx = SpatialContext.GEO;
    SpatialPrefixTree grid = getSpatialPrefixTree(properties);
    boolean docValue = false;
    if (properties.get(DOC_VALUE) != null) {
      docValue = true;
    }
    _strategy = new RecursivePrefixTreeStrategy(grid, fieldNameForThisInstance, docValue);
    _shapeReadWriter = new ShapeReadWriter<SpatialContext>(_ctx);
    addSupportedIndexedShapes(Shape.class);
    addSupportedOperations(SpatialOperation.IsDisjointTo);
    addSupportedOperations(SpatialOperation.Intersects);
    addSupportedOperations(SpatialOperation.IsWithin);
    addSupportedOperations(SpatialOperation.Contains);
  }

  @Override
  public String readTerm(BytesRef byteRef) {
	return byteRef.utf8ToString();
  }
}
