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
package org.apache.blur.mapreduce.lib.v2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexableField;

public enum LUCENE_FIELD_TYPE {

  BinaryDocValuesField(org.apache.lucene.document.BinaryDocValuesField.class, (byte) 0),

  DoubleField(org.apache.lucene.document.DoubleField.class, (byte) 1),

  FloatField(org.apache.lucene.document.FloatField.class, (byte) 2),

  IntField(org.apache.lucene.document.IntField.class, (byte) 3),

  LongField(org.apache.lucene.document.LongField.class, (byte) 4),

  NumericDocValuesField(org.apache.lucene.document.NumericDocValuesField.class, (byte) 5),

  DoubleDocValuesField(org.apache.lucene.document.DoubleDocValuesField.class, (byte) 6),

  FloatDocValuesField(org.apache.lucene.document.FloatDocValuesField.class, (byte) 7),

  SortedDocValuesField(org.apache.lucene.document.SortedDocValuesField.class, (byte) 8),

  SortedSetDocValuesField(org.apache.lucene.document.SortedSetDocValuesField.class, (byte) 9),

  StoredField(org.apache.lucene.document.StoredField.class, (byte) 10),

  StringField(org.apache.lucene.document.StringField.class, (byte) 11),

  TextField(org.apache.lucene.document.TextField.class, (byte) 12);

  private final byte _value;
  private final Class<? extends IndexableField> _clazz;

  private LUCENE_FIELD_TYPE(Class<? extends IndexableField> clazz, byte b) {
    _value = b;
    _clazz = clazz;
  }

  public Class<? extends IndexableField> fieldClass() {
    return _clazz;
  }

  public byte value() {
    return _value;
  }

  public static LUCENE_FIELD_TYPE lookupByValue(byte value) {
    LUCENE_FIELD_TYPE type = _lookupByValue.get(value);
    if (type == null) {
      throw new RuntimeException("Type for [" + value + "] not found.");
    }
    return type;
  }

  public static LUCENE_FIELD_TYPE lookupByClass(Class<? extends IndexableField> value) {
    LUCENE_FIELD_TYPE type = _lookupByClass.get(value);
    if (type == null) {
      throw new RuntimeException("Type for [" + value + "] not found.");
    }
    return type;
  }

  private final static Map<Class<? extends IndexableField>, LUCENE_FIELD_TYPE> _lookupByClass = new ConcurrentHashMap<Class<? extends IndexableField>, LUCENE_FIELD_TYPE>();
  private final static Map<Byte, LUCENE_FIELD_TYPE> _lookupByValue = new ConcurrentHashMap<Byte, LUCENE_FIELD_TYPE>();

  static {
    LUCENE_FIELD_TYPE[] values = LUCENE_FIELD_TYPE.values();
    for (LUCENE_FIELD_TYPE type : values) {
      _lookupByClass.put(type.fieldClass(), type);
      _lookupByValue.put(type.value(), type);
    }
  }
}