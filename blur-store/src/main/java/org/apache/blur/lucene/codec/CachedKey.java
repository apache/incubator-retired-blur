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
package org.apache.blur.lucene.codec;

import org.apache.lucene.index.SegmentInfo;

class CachedKey {

  final long _filePointer;
  final String _name;
  final SegmentInfo _si;

  public CachedKey(String name, long filePointer, SegmentInfo si) {
    _name = name;
    _filePointer = filePointer;
    _si = si;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_filePointer ^ (_filePointer >>> 32));
    result = prime * result + ((_name == null) ? 0 : _name.hashCode());
    result = prime * result + ((_si == null) ? 0 : _si.hashCode());
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
    CachedKey other = (CachedKey) obj;
    if (_filePointer != other._filePointer)
      return false;
    if (_name == null) {
      if (other._name != null)
        return false;
    } else if (!_name.equals(other._name))
      return false;
    if (_si == null) {
      if (other._si != null)
        return false;
    } else if (!_si.equals(other._si))
      return false;
    return true;
  }

}