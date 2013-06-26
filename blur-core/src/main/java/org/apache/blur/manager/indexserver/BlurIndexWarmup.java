package org.apache.blur.manager.indexserver;

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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.IndexReader;


public abstract class BlurIndexWarmup {
  
  
  protected long _warmupBandwidthThrottleBytesPerSec;

  public BlurIndexWarmup(long warmupBandwidthThrottleBytesPerSec) {
    _warmupBandwidthThrottleBytesPerSec = warmupBandwidthThrottleBytesPerSec;
  }

  /**
   * Once the reader has be warmed up, release() must be called on the
   * ReleaseReader even if an exception occurs.
   * 
   * @param table
   *          the table name.
   * @param shard
   *          the shard name.
   * @param reader
   *          thread reader inself.
   * @param isClosed
   *          to check if the shard has been migrated to another node.
   * @param releaseReader
   *          to release the handle on the reader.
   * @throws IOException
   * 
   */
  public void warmBlurIndex(String table, String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {

  }

  /**
   * Once the reader has be warmed up, release() must be called on the
   * ReleaseReader even if an exception occurs.
   * 
   * @param table
   *          the table descriptor.
   * @param shard
   *          the shard name.
   * @param reader
   *          thread reader inself.
   * @param isClosed
   *          to check if the shard has been migrated to another node.
   * @param releaseReader
   *          to release the handle on the reader.
   * @throws IOException
   */
  public void warmBlurIndex(TableDescriptor table, String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    warmBlurIndex(table.name, shard, reader, isClosed, releaseReader);
  }

}
