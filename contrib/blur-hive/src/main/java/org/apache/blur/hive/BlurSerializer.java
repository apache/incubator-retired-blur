/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.hive;

import java.util.List;
import java.util.Map;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazy.LazyVoid;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BlurSerializer {

  public Writable serialize(Object o, ObjectInspector objectInspector, List<String> columnNames,
      List<TypeInfo> columnTypes, Map<String, ColumnDefinition> schema, String family) throws SerDeException {
    BlurRecord blurRecord = new BlurRecord();
    blurRecord.setFamily(family);

    StructObjectInspector soi = (StructObjectInspector) objectInspector;

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    int size = columnNames.size();
    if (outputFieldRefs.size() != size) {
      throw new SerDeException("Number of input columns was different than output columns (in = " + size + " vs out = "
          + outputFieldRefs.size());
    }
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);
    for (int i = 0; i < size; i++) {
      // StructField structFieldRef = outputFieldRefs.get(i);
      Object structFieldData = structFieldsDataAsList.get(i);
      if (structFieldData == null) {
        continue;
      }
      // ObjectInspector fieldOI = structFieldRef.getFieldObjectInspector();
      String columnName = columnNames.get(i);
      String stringValue = toString(structFieldData);
      if (stringValue == null) {
        continue;
      }
      if (columnName.equals(BlurObjectInspectorGenerator.ROWID)) {
        blurRecord.setRowId(stringValue);
      } else if (columnName.equals(BlurObjectInspectorGenerator.RECORDID)) {
        blurRecord.setRecordId(stringValue);
      } else {
        if (columnName.equals(BlurObjectInspectorGenerator.GEO_POINTVECTOR)
            || columnName.equals(BlurObjectInspectorGenerator.GEO_RECURSIVEPREFIX)
            || columnName.equals(BlurObjectInspectorGenerator.GEO_TERMPREFIX)) {
          throw new SerDeException("Not supported yet.");
        } else {
          blurRecord.addColumn(columnName, stringValue);
        }
      }
    }
    return blurRecord;
  }

  private String toString(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof LazyBoolean) {
      return lazyBoolean((LazyBoolean) o);
    } else if (o instanceof LazyByte) {
      return lazyByte((LazyByte) o);
    } else if (o instanceof LazyDate) {
      return lazyDate((LazyDate) o);
    } else if (o instanceof LazyDouble) {
      return lazyDouble((LazyDouble) o);
    } else if (o instanceof LazyFloat) {
      return lazyFloat((LazyFloat) o);
    } else if (o instanceof LazyHiveChar) {
      return lazyHiveChar((LazyHiveChar) o);
    } else if (o instanceof LazyHiveDecimal) {
      return lazyHiveDecimal((LazyHiveDecimal) o);
    } else if (o instanceof LazyHiveVarchar) {
      return lazyHiveVarchar((LazyHiveVarchar) o);
    } else if (o instanceof LazyInteger) {
      return lazyInteger((LazyInteger) o);
    } else if (o instanceof LazyLong) {
      return lazyLong((LazyLong) o);
    } else if (o instanceof LazyShort) {
      return lazyShort((LazyShort) o);
    } else if (o instanceof LazyShort) {
      return lazyString((LazyString) o);
    } else if (o instanceof LazyTimestamp) {
      return lazyTimestamp((LazyTimestamp) o);
    } else if (o instanceof LazyVoid) {
      return null;
    }
    return o.toString();
  }

  private String lazyInteger(LazyInteger o) {
    IntWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    int i = writableObject.get();
    return Integer.toString(i);
  }

  private String lazyLong(LazyLong o) {
    LongWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    long l = writableObject.get();
    return Long.toString(l);
  }

  private String lazyShort(LazyShort o) {
    ShortWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    short s = writableObject.get();
    return Short.toString(s);
  }

  private String lazyString(LazyString o) {
    Text writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    return writableObject.toString();
  }

  private String lazyTimestamp(LazyTimestamp o) {
    throw new RuntimeException("Not implemented.");
  }

  private String lazyHiveVarchar(LazyHiveVarchar o) {
    throw new RuntimeException("Not implemented.");
  }

  private String lazyHiveDecimal(LazyHiveDecimal o) {
    throw new RuntimeException("Not implemented.");
  }

  private String lazyHiveChar(LazyHiveChar o) {
    throw new RuntimeException("Not implemented.");
  }

  private String lazyFloat(LazyFloat o) {
    FloatWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    float f = writableObject.get();
    return Float.toString(f);
  }

  private String lazyDouble(LazyDouble o) {
    DoubleWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    double d = writableObject.get();
    return Double.toString(d);
  }

  private String lazyDate(LazyDate o) {
    throw new RuntimeException("Not implemented.");
  }

  private String lazyByte(LazyByte o) {
    ByteWritable writableObject = o.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    byte b = writableObject.get();
    return Integer.toString(b);
  }

  private String lazyBoolean(LazyBoolean lazyBoolean) {
    BooleanWritable writableObject = lazyBoolean.getWritableObject();
    if (writableObject == null) {
      return null;
    }
    return Boolean.toString(writableObject.get());
  }
}
