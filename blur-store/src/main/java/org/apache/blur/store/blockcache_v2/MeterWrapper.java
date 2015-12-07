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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.core.Meter;

public abstract class MeterWrapper implements Closeable {

  private static final Log LOG = LogFactory.getLog(MeterWrapper.class);

  public static final MeterWrapper NOTHING = new MeterWrapper() {
    @Override
    public void mark() {

    }

    @Override
    public void close() throws IOException {

    }
  };

  private final static Timer _timer;
  private final static ConcurrentMap<String, MeterWrapperCounter> _counterMap = new ConcurrentHashMap<String, MeterWrapperCounter>();

  private static final long DELAY = TimeUnit.SECONDS.toMillis(1);

  static {
    _timer = new Timer("MeterWrapper", true);
    _timer.schedule(getCounterUnLoadTimerTask(), DELAY, DELAY);
  }

  public static interface SimpleMeter {
    void mark(long l);
  }

  public static MeterWrapper wrap(final Meter meter) {
    return wrap(new SimpleMeter() {
      @Override
      public void mark(long l) {
        meter.mark(l);
      }
    });
  }

  public static MeterWrapper wrap(final SimpleMeter meter) {
    final String id = UUID.randomUUID().toString();
    final ThreadLocal<AtomicLong> countThreadLocal = new ThreadLocal<AtomicLong>() {
      @Override
      protected AtomicLong initialValue() {
        AtomicLong counter = new AtomicLong();
        register(id, meter, counter);
        return counter;
      }
    };
    return new MeterWrapper() {
      @Override
      public void mark() {
        countThreadLocal.get().incrementAndGet();
      }

      @Override
      public void close() throws IOException {
        unregister(id);
      }
    };
  }

  private static TimerTask getCounterUnLoadTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          updateMetrics();
        } catch (Throwable t) {
          LOG.error("Unknown error.", t);
        }
      }
    };
  }

  public static void updateMetrics() {
    Collection<MeterWrapperCounter> values = _counterMap.values();
    for (MeterWrapperCounter meterWrapperCounter : values) {
      meterWrapperCounter.markMeter();
    }
  }

  private static void unregister(String id) {
    _counterMap.remove(id);
  }

  private static void register(String id, SimpleMeter meter, AtomicLong counter) {
    {
      _counterMap.putIfAbsent(id, new MeterWrapperCounter(meter));
    }
    {
      _counterMap.get(id).add(counter);
    }
  }

  static class MeterWrapperCounter {
    final SimpleMeter _meter;
    final Set<AtomicLong> _counterSet = Collections.newSetFromMap(new ConcurrentHashMap<AtomicLong, Boolean>());

    MeterWrapperCounter(SimpleMeter meter) {
      _meter = meter;
    }

    void add(AtomicLong counter) {
      _counterSet.add(counter);
    }

    void markMeter() {
      for (AtomicLong count : _counterSet) {
        _meter.mark(count.getAndSet(0));
      }
    }

  }

  public abstract void mark();

}
