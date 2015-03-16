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
package org.apache.blur.server.cache;

import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.thrift.generated.BlurException;

public class ThriftCacheKey<T extends TBase<?, ?>> {

  // * bytes for Object plus 8 bytes per reference plus 8 bytes for timestamp.
  private static final int OVERHEAD = 8 + (5 * 8) + 8;

  private final Map<String, String> _attributes;
  private final String _table;
  private final ShardsKey _shards;
  private final ClassObj<T> _clazz;
  private final ThriftCacheValue<T> _key;

  // not apart of the key for equally
  private final transient long _timestamp;

  public ThriftCacheKey(Map<String, String> userAttributes, String table, ShardsKey shards, T t, ClassObj<T> clazz)
      throws BlurException {
    _timestamp = System.nanoTime();
    _clazz = clazz;
    _attributes = userAttributes;
    _table = table;
    _shards = shards;
    _key = new ThriftCacheValue<T>(t);
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public String getTable() {
    return _table;
  }

  public int size() {
    return _key.size() + OVERHEAD;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_attributes == null) ? 0 : _attributes.hashCode());
    result = prime * result + ((_clazz == null) ? 0 : _clazz.hashCode());
    result = prime * result + ((_key == null) ? 0 : _key.hashCode());
    result = prime * result + ((_shards == null) ? 0 : _shards.hashCode());
    result = prime * result + ((_table == null) ? 0 : _table.hashCode());
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
    ThriftCacheKey<?> other = (ThriftCacheKey<?>) obj;
    if (_attributes == null) {
      if (other._attributes != null)
        return false;
    } else if (!_attributes.equals(other._attributes))
      return false;
    if (_clazz == null) {
      if (other._clazz != null)
        return false;
    } else if (!_clazz.equals(other._clazz))
      return false;
    if (_key == null) {
      if (other._key != null)
        return false;
    } else if (!_key.equals(other._key))
      return false;
    if (_shards == null) {
      if (other._shards != null)
        return false;
    } else if (!_shards.equals(other._shards))
      return false;
    if (_table == null) {
      if (other._table != null)
        return false;
    } else if (!_table.equals(other._table))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ThriftCacheKey [_attributes=" + _attributes + ", _table=" + _table + ", _shards=" + _shards + ", _clazz="
        + _clazz + ", _key=" + _key + ", _timestamp=" + _timestamp + "]";
  }

}
