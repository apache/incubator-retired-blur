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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.store.blockcache_v2.MeterWrapper.SimpleMeter;
import org.junit.Test;

public class MeterWrapperTest {

  @Test
  public void test() throws InterruptedException {
    final AtomicLong totalCount = new AtomicLong();
    SimpleMeter simpleMeter = new MeterWrapper.SimpleMeter() {
      @Override
      public void mark(long l) {
        totalCount.addAndGet(l);
      }
    };
    final MeterWrapper wrap = MeterWrapper.wrap(simpleMeter);
    int threadCount = 4;
    final int max = Integer.MAX_VALUE / 10;
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          for (int l = 0; l < max; l++) {
            wrap.mark();
          }
        }
      });
      threads.add(thread);
    }
    long s = System.nanoTime();
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    long e = System.nanoTime();
    System.out.println("Time [" + (e - s) / 1000000.0 + " ms]");
    MeterWrapper.updateMetrics();
    assertEquals(max * 4, totalCount.get());
  }
}
