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
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WARMUP_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_WARMUP_THROTTLE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_WARMUP_THREAD_COUNT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.IndexReader;

public abstract class BlurIndexWarmup {

  private static final Log LOG = LogFactory.getLog(BlurIndexWarmup.class);

  protected long _warmupBandwidthThrottleBytesPerSec;

  public BlurIndexWarmup(long warmupBandwidthThrottleBytesPerSec) {
    _warmupBandwidthThrottleBytesPerSec = warmupBandwidthThrottleBytesPerSec;
  }

  public static BlurIndexWarmup getIndexWarmup(BlurConfiguration configuration) {
    long totalThrottle = configuration.getLong(BLUR_SHARD_INDEX_WARMUP_THROTTLE, 30000000);
    int totalThreadCount = configuration.getInt(BLUR_SHARD_WARMUP_THREAD_COUNT, 30000000);
    long warmupBandwidthThrottleBytesPerSec = totalThrottle / totalThreadCount;
    if (warmupBandwidthThrottleBytesPerSec <= 0) {
      LOG.warn("Invalid values of either [{0} = {1}] or [{2} = {3}], needs to be greater then 0",
          BLUR_SHARD_INDEX_WARMUP_THROTTLE, totalThrottle, BLUR_SHARD_WARMUP_THREAD_COUNT, totalThreadCount);
    }
    
    String blurFilterCacheClass = configuration.get(BLUR_SHARD_INDEX_WARMUP_CLASS);
    if (blurFilterCacheClass != null && blurFilterCacheClass.isEmpty()) {
      if (!blurFilterCacheClass.equals("org.apache.blur.manager.indexserver.DefaultBlurIndexWarmup")) {
        try {
          Class<?> clazz = Class.forName(blurFilterCacheClass);
          Constructor<?> constructor = clazz.getConstructor(new Class[]{Long.TYPE});
          return (BlurIndexWarmup) constructor.newInstance(warmupBandwidthThrottleBytesPerSec);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return new DefaultBlurIndexWarmup(warmupBandwidthThrottleBytesPerSec);
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
   *          thread reader itself.
   * @param isClosed
   *          to check if the shard has been migrated to another node.
   * @param releaseReader
   *          to release the handle on the reader.
   * @throws IOException
   */
  public abstract void warmBlurIndex(TableDescriptor table, String shard, IndexReader reader, AtomicBoolean isClosed,
      ReleaseReader releaseReader, AtomicLong pause) throws IOException;

}
