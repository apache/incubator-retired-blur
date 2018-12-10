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
package org.apache.blur.lucene.fst;

import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.SIZE;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.BlurConfiguration;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class ByteArrayPrimitiveFactory extends ByteArrayFactory {

  private static final String FST = "FST";

  private final AtomicLong _size = new AtomicLong();

  public ByteArrayPrimitiveFactory(BlurConfiguration configuration) {
    super(configuration);
    Metrics.newGauge(new MetricName(ORG_APACHE_BLUR, FST, SIZE), new Gauge<Long>() {
      @Override
      public Long value() {
        return _size.get();
      }
    });
  }

  @Override
  public ByteArray newByteArray(int size) {
    _size.addAndGet(size);
    return new ByteArrayPrimitive(size);
  }

}
