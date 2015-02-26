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
import java.util.TreeMap;

import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.user.User;

public class ThriftCacheKey<T extends TBase<?, ?>> {

  private final String _username;
  private final Map<String, String> _attributes;
  private final String _table;
  private final T _t;
  private final String _clazz;

  public ThriftCacheKey(User user, String table, T t, Class<T> clazz) {
    _clazz = clazz.getName();
    if (user != null) {
      _username = user.getUsername();
      Map<String, String> attributes = user.getAttributes();
      if (attributes == null) {
        _attributes = attributes;
      } else {
        _attributes = new TreeMap<String, String>(user.getAttributes());
      }
    } else {
      _username = null;
      _attributes = null;
    }
    _table = table;
    _t = t;
  }

  public String getTable() {
    return _table;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_attributes == null) ? 0 : _attributes.hashCode());
    result = prime * result + ((_clazz == null) ? 0 : _clazz.hashCode());
    result = prime * result + ((_t == null) ? 0 : _t.hashCode());
    result = prime * result + ((_table == null) ? 0 : _table.hashCode());
    result = prime * result + ((_username == null) ? 0 : _username.hashCode());
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
    if (_t == null) {
      if (other._t != null)
        return false;
    } else if (!_t.equals(other._t))
      return false;
    if (_table == null) {
      if (other._table != null)
        return false;
    } else if (!_table.equals(other._table))
      return false;
    if (_username == null) {
      if (other._username != null)
        return false;
    } else if (!_username.equals(other._username))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ThriftCacheKey [_username=" + _username + ", _attributes=" + _attributes + ", _table=" + _table + ", _t="
        + _t + ", _clazz=" + _clazz + "]";
  }

}
