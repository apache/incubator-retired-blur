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
package org.apache.blur.trace;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.trace.Trace.TraceId;
import org.json.JSONException;
import org.junit.Test;

public class TraceTest {

  @Test
  public void testTrace() throws IOException {
    Trace.setStorage(new BaseTraceStorage(new BlurConfiguration()) {
      @Override
      public void store(TraceCollector collector) {
        assertEquals("test", collector.getId().getRootId());
        assertEquals(3, collector.getTraces().size());
      }

      @Override
      public void close() throws IOException {
        
      }
    });
    Trace.setupTrace("test", null);
    Tracer trace = Trace.trace("1");
    long meth1;
    try {
      meth1 = meth1();
    } finally {
      trace.done();
    }
    System.out.println(meth1);
    Trace.tearDownTrace();
  }

  @Test
  public void testNoTrace() {
    Tracer trace = Trace.trace("1");
    long meth1;
    try {
      meth1 = meth1();
    } finally {
      trace.done();
    }
    System.out.println(meth1);
  }

  @Test
  public void testTraceThreadRunnable() throws InterruptedException, IOException {
    final AtomicLong count = new AtomicLong();
    Trace.setStorage(new BaseTraceStorage(new BlurConfiguration()) {
      @Override
      public void store(TraceCollector collector) {
        try {
          System.out.println(collector.toJsonObject());
        } catch (JSONException e) {
          e.printStackTrace();
        }
        TraceId id = collector.getId();
        assertEquals("test", id.getRootId());
        assertEquals(5, collector.getTraces().size());
        count.addAndGet(collector.getTraces().size());
      }

      @Override
      public void close() throws IOException {
        
      }
    });

    Trace.setupTrace("test", "1");

    final Runnable runnable = new Runnable() {
      @Override
      public void run() {
        Tracer trace = Trace.trace("1");
        long meth1;
        try {
          meth1 = meth1();
        } finally {
          trace.done();
        }
        System.out.println(meth1);
      }
    };
    Thread thread = new Thread(Trace.getRunnable(runnable));
    thread.start();
    Tracer trace = Trace.trace("1");
    long meth1;
    try {
      meth1 = meth1();
    } finally {
      trace.done();
    }
    System.out.println(meth1);
    thread.join();
    Trace.tearDownTrace();

    assertEquals(5, count.get());
  }

  private static long meth1() {
    Tracer trace = Trace.trace("2");
    try {
      return meth2();
    } finally {
      trace.done();
    }
  }

  private static long meth2() {
    Tracer trace = Trace.trace("3");
    try {
      return meth3();
    } finally {
      trace.done();
    }
  }

  private static long meth3() {
    long t = 1;
    for (long i = 1; i < 10000; i++) {
      t += i;
    }
    return t;
  }
}
