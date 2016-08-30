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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thrift.generated.Column;
import org.apache.blur.utils.ThreadValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class DateFieldTypeDefinition extends NumericFieldTypeDefinition {

  public static final String TIME_UNIT = "timeUnit";
  public static final String DATE_FORMAT = "dateFormat";
  public static final String NAME = "date";
  private FieldType _typeNotStored;
  private ThreadValue<SimpleDateFormat> _simpleDateFormat;
  private TimeUnit _timeUnit = TimeUnit.SECONDS;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    final String dateFormat = properties.get(DATE_FORMAT);
    if (dateFormat == null) {
      throw new RuntimeException("The property [" + DATE_FORMAT + "] can not be null.");
    }
    final String timeUnitStr = properties.get(TIME_UNIT);
    if (timeUnitStr != null) {
      _timeUnit = TimeUnit.valueOf(timeUnitStr.trim().toUpperCase());
    }
    _simpleDateFormat = new ThreadValue<SimpleDateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(dateFormat);
      }
    };
    String precisionStepStr = properties.get(NUMERIC_PRECISION_STEP);
    if (precisionStepStr != null) {
      _precisionStep = Integer.parseInt(precisionStepStr);
      _typeNotStored = new FieldType(LongField.TYPE_NOT_STORED);
      _typeNotStored.setNumericPrecisionStep(_precisionStep);
      _typeNotStored.freeze();
    } else {
      _typeNotStored = LongField.TYPE_NOT_STORED;
    }
  }

  private long parseDate(String dateStr) {
    SimpleDateFormat simpleDateFormat = _simpleDateFormat.get();
    try {
      Date date = simpleDateFormat.parse(dateStr);
      return _timeUnit.convert(date.getTime(), TimeUnit.MILLISECONDS);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    long date = parseDate(column.getValue());
    LongField field = new LongField(name, date, _typeNotStored);
    StoredField storedField = new StoredField(name, column.getValue());
    List<Field> fields = new ArrayList<Field>();
    fields.add(field);
    fields.add(storedField);
    if (isSortEnable()) {
      fields.add(new NumericDocValuesField(name, date));
    }
    return fields;
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    long date = parseDate(column.getValue());
    LongField field = new LongField(name, date, _typeNotStored);
    if (isSortEnable()) {
      return addSort(name, date, field);
    }
    return makeIterable(field);
  }

  @Override
  public Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    long p1 = parseDate(part1);
    long p2 = parseDate(part2);
    return NumericRangeQuery.newLongRange(field, _precisionStep, p1, p2, startInclusive, endInclusive);
  }

  @Override
  public SortField getSortField(boolean reverse) {
    if (reverse) {
      return new SortField(getFieldName(), Type.LONG, reverse);
    }
    return new SortField(getFieldName(), Type.LONG);
  }

  @Override
  public String readTerm(BytesRef byteRef) {
	  if(NumericUtils.getPrefixCodedLongShift(byteRef) == 0)
		  return _simpleDateFormat.get().format(new Date(NumericUtils.getPrefixCodedLongShift(byteRef)));
	  return null;
  }
}
