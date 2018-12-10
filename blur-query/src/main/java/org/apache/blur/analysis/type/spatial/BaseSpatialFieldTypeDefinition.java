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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.blur.analysis.type.CustomFieldTypeDefinition;
import org.apache.blur.analysis.type.MultiValuedNotAllowedException;
import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

public abstract class BaseSpatialFieldTypeDefinition extends CustomFieldTypeDefinition {

  public static final String QUAD = "Quad(";
  public static final String END = ")";
  public static final String GEO_HASH = "GeoHash(";
  public static final String GEOHASH_PREFIX_TREE = "GeohashPrefixTree";
  public static final String QUAD_PREFIX_TREE = "QuadPrefixTree";
  public static final String SPATIAL_PREFIX_TREE = "spatialPrefixTree";
  public static final String MAX_LEVELS = "maxLevels";

  protected SpatialStrategy _strategy;
  protected SpatialContext _ctx;
  protected SpatialPrefixTree _grid;
  protected ShapeReadWriter<SpatialContext> _shapeReadWriter;
  protected List<SpatialOperation> _supportedOperations;
  protected List<Class<? extends Shape>> _supportedIndexedShapes;

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    synchronized (_strategy) {
      String name = getName(family, column.getName());
      if (!_strategy.getFieldName().equals(name)) {
        throw new RuntimeException("Strategy name and column name do not match.");
      }
      List<Field> fields = new ArrayList<Field>();
      Shape shape = getShape(column);
      checkShape(shape);
      for (Field f : _strategy.createIndexableFields(shape)) {
        fields.add(f);
      }
      fields.add(new StoredField(name, column.getValue()));
      return fields;
    }
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    synchronized (_strategy) {
      String name = getName(family, column.getName(), subName);
      if (!_strategy.getFieldName().equals(name)) {
        throw new RuntimeException("Strategy name and column name do not match.");
      }
      List<Field> fields = new ArrayList<Field>();
      Shape shape = getShape(column);
      checkShape(shape);
      for (Field f : _strategy.createIndexableFields(shape)) {
        fields.add(f);
      }
      return fields;
    }
  }

  @Override
  public Query getCustomQuery(String text) {
    SpatialArgs args = SpatialArgsParser.parse(text, _shapeReadWriter);
    checkSpatialArgs(args);
    synchronized (_strategy) {
      return _strategy.makeQuery(args);
    }
  }

  protected void checkShape(Shape shape) {
    for (Class<? extends Shape> shapeType : _supportedIndexedShapes) {
      if (shapeType.isInstance(shape)) {
        return;
      }
    }
    throw new IllegalArgumentException(_strategy.getClass().getName() + " only supports [" + _supportedIndexedShapes
        + "] operation.");
  }

  protected void checkSpatialArgs(SpatialArgs args) {
    SpatialOperation operation = args.getOperation();
    for (SpatialOperation so : _supportedOperations) {
      if (operation == so) {
        return;
      }
    }
    throw new IllegalArgumentException(_strategy.getClass().getName() + " only supports [" + _supportedOperations
        + "] operation.");
  }

  protected Shape getShape(Column column) {
    return _shapeReadWriter.readShape(column.getValue());
  }

  protected SpatialPrefixTree getSpatialPrefixTree(Map<String, String> properties) {
    String spatialPrefixTreeStr = properties.get(SPATIAL_PREFIX_TREE);
    if (spatialPrefixTreeStr.equals(GEOHASH_PREFIX_TREE)) {
      int maxLevels = getMaxLevels(properties);
      return new GeohashPrefixTree(_ctx, maxLevels);
    } else if (spatialPrefixTreeStr.equals(QUAD_PREFIX_TREE)) {
      int maxLevels = getMaxLevels(properties);
      return new QuadPrefixTree(_ctx, maxLevels);
    } else {
      throw new RuntimeException("Unknown spatialPrefixTreeStr [" + spatialPrefixTreeStr + "]");
    }
  }

  protected int getMaxLevels(Map<String, String> properties) {
    String maxLevelsStr = properties.get(MAX_LEVELS);
    int maxLevels = 11;
    if (maxLevelsStr != null) {
      maxLevels = Integer.parseInt(maxLevelsStr.trim());
    }
    return maxLevels;
  }

  public List<SpatialOperation> getSupportedOperations() {
    return _supportedOperations;
  }

  public void setSupportedOperations(List<SpatialOperation> supportedOperations) {
    _supportedOperations = supportedOperations;
  }

  public void addSupportedOperations(SpatialOperation so) {
    if (_supportedOperations == null) {
      _supportedOperations = new ArrayList<SpatialOperation>();
    }
    _supportedOperations.add(so);
  }

  public List<Class<? extends Shape>> getSupportedIndexedShapes() {
    return _supportedIndexedShapes;
  }

  public void setSupportedIndexedShapes(List<Class<? extends Shape>> supportedIndexedShapes) {
    _supportedIndexedShapes = supportedIndexedShapes;
  }

  public void addSupportedIndexedShapes(Class<? extends Shape> c) {
    if (_supportedIndexedShapes == null) {
      _supportedIndexedShapes = new ArrayList<Class<? extends Shape>>();
    }
    _supportedIndexedShapes.add(c);
  }

  @Override
  public void setMultiValueField(boolean multiValueField) {
    if (!multiValueField) {
      super.setMultiValueField(multiValueField);
    } else {
      throw new MultiValuedNotAllowedException("Field type [" + getName() + "] can not multi valued.");
    }
  }

}
