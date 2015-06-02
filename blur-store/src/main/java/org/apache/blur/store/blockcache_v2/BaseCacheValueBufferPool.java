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

import static org.apache.blur.metrics.MetricsConstants.CACHE_POOL;
import static org.apache.blur.metrics.MetricsConstants.CREATED;
import static org.apache.blur.metrics.MetricsConstants.DESTROYED;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.REUSED;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.apache.blur.store.blockcache_v2.BaseCache.STORE;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public abstract class BaseCacheValueBufferPool implements Closeable {

  protected final STORE _store;
  protected final Meter _reused;
  protected final Meter _detroyed;
  protected final Meter _created;

  public BaseCacheValueBufferPool(STORE store) {
    _store = store;
    _created = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, CREATED), CREATED, TimeUnit.SECONDS);
    _reused = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, REUSED), REUSED, TimeUnit.SECONDS);
    _detroyed = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, CACHE_POOL, DESTROYED), DESTROYED, TimeUnit.SECONDS);
  }

  public abstract CacheValue getCacheValue(int cacheBlockSize);

  public abstract void returnToPool(CacheValue cacheValue);

}
