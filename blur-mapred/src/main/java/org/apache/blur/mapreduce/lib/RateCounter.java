package org.apache.blur.mapreduce.lib;

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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Counter;

/**
 * This turns a standard hadoop counter into a rate counter.
 */
public class RateCounter {

  private final Counter _counter;
  private final long _reportTime;
  private long _lastReport;
  private long _count = 0;

  public RateCounter(Counter counter) {
    _counter = counter;
    _lastReport = System.nanoTime();
    _reportTime = TimeUnit.SECONDS.toNanos(5);
  }

  public void mark() {
    mark(1l);
  }

  public void mark(long n) {
    long now = System.nanoTime();
    if (_lastReport + _reportTime < now) {
      long seconds = TimeUnit.NANOSECONDS.toSeconds(now - _lastReport);
      long rate = _count / seconds;
      _counter.setValue(rate);
      _lastReport = System.nanoTime();
      _count = 0;
    }
    _count += n;
  }

  public void close() {
    _counter.setValue(0);
  }

}
