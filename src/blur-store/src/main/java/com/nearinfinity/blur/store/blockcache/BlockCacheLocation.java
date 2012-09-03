package com.nearinfinity.blur.store.blockcache;

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
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockCacheLocation {

  private int _block;
  private int _slabId;
  private long _lastAccess = System.currentTimeMillis();
  private long _accesses;
  private AtomicBoolean _removed = new AtomicBoolean(false);

  public void setBlock(int block) {
    _block = block;
  }

  public void setSlabId(int slabId) {
    _slabId = slabId;
  }

  public int getBlock() {
    return _block;
  }

  public int getSlabId() {
    return _slabId;
  }

  public void touch() {
    _lastAccess = System.currentTimeMillis();
    _accesses++;
  }

  public long getLastAccess() {
    return _lastAccess;
  }

  public long getNumberOfAccesses() {
    return _accesses;
  }

  public boolean isRemoved() {
    return _removed.get();
  }

  public void setRemoved(boolean removed) {
    _removed.set(removed);
  }

}