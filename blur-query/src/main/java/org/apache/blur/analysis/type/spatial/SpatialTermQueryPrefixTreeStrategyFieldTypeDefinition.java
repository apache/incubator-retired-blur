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

import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.ShapeReadWriter;
import com.spatial4j.core.shape.Point;

public class SpatialTermQueryPrefixTreeStrategyFieldTypeDefinition extends BaseSpatialFieldTypeDefinition {

  public static final String NAME = "geo-termprefix";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties) {
    _ctx = SpatialContext.GEO;
    SpatialPrefixTree grid = getSpatialPrefixTree(properties);
    _strategy = new TermQueryPrefixTreeStrategy(grid, fieldNameForThisInstance);
    _shapeReadWriter = new ShapeReadWriter<SpatialContext>(_ctx);
    addSupportedIndexedShapes(Point.class);
    addSupportedOperations(SpatialOperation.Intersects);
  }
}
