package org.apache.blur.store.blockcache;

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
public class BlockCacheKey implements Cloneable {

  private long _block;
  private int _file;

  public long getBlock() {
    return _block;
  }

  public int getFile() {
    return _file;
  }

  public void setBlock(long block) {
    _block = block;
  }

  public void setFile(int file) {
    _file = file;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_block ^ (_block >>> 32));
    result = prime * result + _file;
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
    BlockCacheKey other = (BlockCacheKey) obj;
    if (_block != other._block)
      return false;
    if (_file != other._file)
      return false;
    return true;
  }

  @Override
  public BlockCacheKey clone() {
    try {
      return (BlockCacheKey) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
