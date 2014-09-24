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
package org.apache.blur.command;

import java.util.ArrayList;
import java.util.List;

public class BlurArray {

  private List<Object> _values = new ArrayList<Object>();

  public BlurArray() {

  }

  public BlurArray(BlurArray array) {
    _values.addAll(array._values);
  }

  public BlurArray(List<? extends Object> vals) {
    _values .addAll(vals);
  }
  
  public List<? extends Object> asList() {
    return _values;
  }

  public void clear() {
    _values.clear();
  }

  public String getString(int index) {
    return (String) getObject(index);
  }

  public void put(String value) {
    put((Object) value);
  }

  public void put(int index, String value) {
    put(index, (Object) value);
  }

  public Integer getInteger(int index) {
    return (Integer) getObject(index);
  }

  public void put(Integer value) {
    put((Object) value);
  }

  public void put(int index, Integer value) {
    put(index, (Object) value);
  }

  public Short getShort(int index) {
    return (Short) getObject(index);
  }

  public void put(Short value) {
    put((Object) value);
  }

  public void put(int index, Short value) {
    put(index, (Object) value);
  }

  public Long getLong(int index) {
    return (Long) getObject(index);
  }

  public void put(Long value) {
    put((Object) value);
  }

  public void put(int index, Long value) {
    put(index, (Object) value);
  }

  public Double getDouble(int index) {
    return (Double) getObject(index);
  }

  public void put(Double value) {
    put((Object) value);
  }

  public void put(int index, Double value) {
    put(index, (Object) value);
  }

  public Float getFloat(int index) {
    return (Float) getObject(index);
  }

  public void put(Float value) {
    put((Object) value);
  }

  public void put(int index, Float value) {
    put(index, (Object) value);
  }

  public byte[] getBinary(int index) {
    return (byte[]) getObject(index);
  }

  public void put(byte[] value) {
    put((Object) value);
  }

  public void put(int index, byte[] value) {
    put(index, (Object) value);
  }

  public Boolean getBoolean(int index) {
    return (Boolean) getObject(index);
  }

  public void put(Boolean value) {
    put((Object) value);
  }

  public void put(int index, Boolean value) {
    put(index, (Object) value);
  }

  public BlurObject getBlurObject(int index) {
    return (BlurObject) getObject(index);
  }

  public void put(BlurObject value) {
    put((Object) value);
  }

  public void put(int index, BlurObject value) {
    put(index, (Object) value);
  }

  public BlurArray getBlurArray(int index) {
    return (BlurArray) getObject(index);
  }

  public void put(BlurArray value) {
    put((Object) value);
  }

  public void put(int index, BlurArray value) {
    put(index, (Object) value);
  }

  public void put(Object value) {
    BlurObject.checkType(value);
    _values.add(value);
  }

  public void put(int index, Object value) {
    BlurObject.checkType(value);
    int sizeNeeded = index + 1;
    while (_values.size() < sizeNeeded) {
      _values.add(null);
    }
    _values.set(index, value);
  }

  @Override
  public String toString() {
    return toString(0);
  }

  public String toString(int i) {
    StringBuilder builder = new StringBuilder();
    builder.append('[');
    boolean comma = false;
    for (Object value : _values) {
      if (comma) {
        builder.append(',');
      }
      comma = true;
      if (i > 0) {
        builder.append('\n');
        for (int j = 0; j < i; j++) {
          builder.append(' ');
        }
      }
      if (value instanceof BlurObject) {
        builder.append(((BlurObject) value).toString(i > 0 ? i + 1 : 0));
      } else if (value instanceof BlurArray) {
        builder.append(((BlurArray) value).toString(i > 0 ? i + 1 : 0));
      } else {
        builder.append(BlurObject.stringify(value));
      }
    }
    if (i > 0) {
      builder.append('\n');
      for (int j = 0; j < i - 1; j++) {
        builder.append(' ');
      }
    }
    builder.append(']');
    return builder.toString();
  }

  public int length() {
    return _values.size();
  }

  public Object getObject(int i) {
    return _values.get(i);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(int i) {
    return (T) _values.get(i);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_values == null) ? 0 : _values.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BlurArray other = (BlurArray) obj;
    if (_values == null) {
      if (other._values != null)
        return false;
    } else if (!_values.equals(other._values))
      return false;
    return true;
  }

}
