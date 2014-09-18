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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.vector.PointVectorStrategy;
import org.apache.lucene.util.BytesRef;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;

public class SpatialPointVectorStrategyFieldTypeDefinition extends BaseSpatialFieldTypeDefinition {

  public static final String NAME = "geo-pointvector";

  private final Analyzer _keywordAnalyzer = new KeywordAnalyzer();
  
  private Collection<String> _alternateFieldNames;
  

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    _ctx = SpatialContext.GEO;
    _strategy = new PointVectorStrategy(_ctx, fieldNameForThisInstance);
    _shapeReadWriter = new ShapeReadWriter<SpatialContext>(_ctx);
    _alternateFieldNames = Arrays.asList(fieldNameForThisInstance + PointVectorStrategy.SUFFIX_X,
        fieldNameForThisInstance + PointVectorStrategy.SUFFIX_Y);
    addSupportedIndexedShapes(Point.class);
    addSupportedOperations(SpatialOperation.Intersects);
  }

  @Override
  public Collection<String> getAlternateFieldNames() {
    return _alternateFieldNames;
  }
  
  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    return _keywordAnalyzer;
  }
  
  @Override
  public String readTerm(BytesRef byteRef) {
	return byteRef.utf8ToString();
  }

}
