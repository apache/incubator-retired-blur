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
package org.apache.blur.store.blockcache_v2;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

public abstract class IndexInputCache implements Closeable {

  protected final long _fileLength;
  protected final int _cacheBlockSize;
  protected final MeterWrapper _hits;

  protected IndexInputCache(long fileLength, int cacheBlockSize, MeterWrapper hits) {
    _fileLength = fileLength;
    _cacheBlockSize = cacheBlockSize;
    _hits = hits;
  }

  public abstract void put(long blockId, CacheValue cacheValue);

  public abstract CacheValue get(long blockId);

  protected void hit() {
    _hits.mark();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_hits);
  }

}
