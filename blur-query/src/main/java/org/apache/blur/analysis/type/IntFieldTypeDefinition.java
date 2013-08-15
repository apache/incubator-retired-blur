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
import java.util.Map;

import org.apache.blur.thrift.generated.Column;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;

public class IntFieldTypeDefinition extends NumericFieldTypeDefinition {

  public static final String NAME = "int";
  private FieldType _typeStored;
  private FieldType _typeNotStored;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties) {
    String precisionStepStr = properties.get(NUMERIC_PRECISION_STEP);
    if (precisionStepStr != null) {
      _precisionStep = Integer.parseInt(precisionStepStr);
      _typeStored = new FieldType(IntField.TYPE_STORED);
      _typeStored.setNumericPrecisionStep(_precisionStep);
      _typeStored.freeze();
      _typeNotStored = new FieldType(IntField.TYPE_NOT_STORED);
      _typeNotStored.setNumericPrecisionStep(_precisionStep);
      _typeNotStored.freeze();
    } else {
      _typeStored = IntField.TYPE_STORED;
      _typeNotStored = IntField.TYPE_NOT_STORED;
    }
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    IntField field = new IntField(name, Integer.parseInt(column.getValue()), _typeStored);
    return makeIterable(field);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    return makeIterable(new IntField(name, Integer.parseInt(column.getValue()), _typeNotStored));
  }

  @Override
  public Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    int p1 = Integer.parseInt(part1);
    int p2 = Integer.parseInt(part2);
    return NumericRangeQuery.newIntRange(field, _precisionStep, p1, p2, startInclusive, endInclusive);
  }
}
