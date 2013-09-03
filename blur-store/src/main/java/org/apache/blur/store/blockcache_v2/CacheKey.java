/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.blockcache_v2;

public class CacheKey implements Cloneable {

  private long _fileId;
  private long _blockId;

  public CacheKey() {

  }

  public CacheKey(long fileId, long blockId) {
    _fileId = fileId;
    _blockId = blockId;
  }

  public long getFileId() {
    return _fileId;
  }

  public void setFileId(long fileId) {
    _fileId = fileId;
  }

  public long getBlockId() {
    return _blockId;
  }

  public void setBlockId(long blockId) {
    _blockId = blockId;
  }

  @Override
  public CacheKey clone() {
    try {
      return (CacheKey) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_blockId ^ (_blockId >>> 32));
    result = prime * result + (int) (_fileId ^ (_fileId >>> 32));
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
    CacheKey other = (CacheKey) obj;
    if (_blockId != other._blockId)
      return false;
    if (_fileId != other._fileId)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "CacheKey [_fileId=" + _fileId + ", _blockId=" + _blockId + "]";
  }

}
