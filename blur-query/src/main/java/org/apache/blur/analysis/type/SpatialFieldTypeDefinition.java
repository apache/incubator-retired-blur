package org.apache.blur.analysis.type;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.type.spatial.SpatialArgsParser;
import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.ShapeReadWriter;
import com.spatial4j.core.shape.Shape;

public class SpatialFieldTypeDefinition extends FieldTypeDefinition {

  private static final String MAX_LEVELS = "maxLevels";
  public static final String NAME = "spatial";

  private SpatialStrategy _strategy;
  private SpatialContext _ctx;
  private ShapeReadWriter<SpatialContext> _shapeReadWriter;
  private final Analyzer _queryAnalyzer = new KeywordAnalyzer();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties) {
    _ctx = SpatialContext.GEO;
    String maxLevelsStr = properties.get(MAX_LEVELS);
    int maxLevels = 11;
    if (maxLevelsStr != null) {
      maxLevels = Integer.parseInt(maxLevelsStr.trim());
    }
    SpatialPrefixTree grid = new GeohashPrefixTree(_ctx, maxLevels);
    _strategy = new RecursivePrefixTreeStrategy(grid, fieldNameForThisInstance);
    _shapeReadWriter = new ShapeReadWriter<SpatialContext>(_ctx);
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    if (!_strategy.getFieldName().equals(name)) {
      throw new RuntimeException("Strategy name and column name do not match.");
    }
    List<Field> fields = new ArrayList<Field>();
    Shape shape = getShape(column);
    for (Field f : _strategy.createIndexableFields(shape)) {
      fields.add(f);
    }
    fields.add(new StoredField(name, cleanup(shape)));
    return fields;
  }

  private String cleanup(Shape shape) {
    return _shapeReadWriter.writeShape(shape);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    if (!_strategy.getFieldName().equals(name)) {
      throw new RuntimeException("Strategy name and column name do not match.");
    }
    List<Field> fields = new ArrayList<Field>();
    Shape shape = getShape(column);
    for (Field f : _strategy.createIndexableFields(shape)) {
      fields.add(f);
    }
    return fields;
  }

  private Shape getShape(Column column) {
    return _shapeReadWriter.readShape(column.getValue());
  }
  
  @Override
  public Query getCustomQuery(String text) {
    SpatialArgs args = SpatialArgsParser.parse(text, _shapeReadWriter);
    return _strategy.makeQuery(args);
  }

  @Override
  public FieldType getStoredFieldType() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public FieldType getNotStoredFieldType() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Analyzer getAnalyzerForIndex() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Analyzer getAnalyzerForQuery() {
    return _queryAnalyzer;
  }

  @Override
  public boolean checkSupportForFuzzyQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForWildcardQuery() {
    return false;
  }

  @Override
  public boolean checkSupportForPrefixQuery() {
    return false;
  }

  @Override
  public boolean isNumeric() {
    return false;
  }

  @Override
  public boolean checkSupportForCustomQuery() {
    return true;
  }
  
}
