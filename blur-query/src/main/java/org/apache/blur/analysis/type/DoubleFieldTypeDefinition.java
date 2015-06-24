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
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class DoubleFieldTypeDefinition extends NumericFieldTypeDefinition {

  public static final String NAME = "double";
  private FieldType _typeStored;
  private FieldType _typeNotStored;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    String precisionStepStr = properties.get(NUMERIC_PRECISION_STEP);
    if (precisionStepStr != null) {
      _precisionStep = Integer.parseInt(precisionStepStr);
      _typeStored = new FieldType(DoubleField.TYPE_STORED);
      _typeStored.setNumericPrecisionStep(_precisionStep);
      _typeStored.freeze();
      _typeNotStored = new FieldType(DoubleField.TYPE_NOT_STORED);
      _typeNotStored.setNumericPrecisionStep(_precisionStep);
      _typeNotStored.freeze();
    } else {
      _typeStored = DoubleField.TYPE_STORED;
      _typeNotStored = DoubleField.TYPE_NOT_STORED;
    }
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    double value = Double.parseDouble(column.getValue());
    DoubleField field = new DoubleField(name, value, _typeStored);
    if (isSortEnable()) {
      return addSort(name, Double.doubleToRawLongBits(value), field);
    }
    return makeIterable(field);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    double value = Double.parseDouble(column.getValue());
    DoubleField field = new DoubleField(name, value, _typeNotStored);
    if (isSortEnable()) {
      return addSort(name, Double.doubleToRawLongBits(value), field);
    }
    return makeIterable(field);
  }

  @Override
  public Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    double p1 = parseDouble(part1);
    double p2 = parseDouble(part2);
    return NumericRangeQuery.newDoubleRange(field, _precisionStep, p1, p2, startInclusive, endInclusive);
  }

  @Override
  public SortField getSortField(boolean reverse) {
    if (reverse) {
      return new SortField(getFieldName(), Type.DOUBLE, reverse);
    }
    return new SortField(getFieldName(), Type.DOUBLE);
  }

  private double parseDouble(String number) {
    if (number.toLowerCase().equals(MIN)) {
      return Double.MIN_VALUE;
    } else if (number.toLowerCase().equals(MAX)) {
      return Double.MAX_VALUE;
    } else {
      return Double.parseDouble(number);
    }
  }

  @Override
  public String readTerm(BytesRef byteRef) {
	  if(NumericUtils.getPrefixCodedLongShift(byteRef) == 0)
		  return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(byteRef))+"";
	  return null;
  }
  
}
