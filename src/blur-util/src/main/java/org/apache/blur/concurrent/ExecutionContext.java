package org.apache.blur.concurrent;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionContext {

  public static class RecordTime {
    public long _now;
    public Enum<?> _e;
    public long _timeNs;
    public int _call;
    public Object[] _args;

    public RecordTime(Enum<?> e, long now, long start, int call, Object[] args) {
      _e = e;
      _timeNs = now - start;
      _call = call;
      _now = now;
      _args = args;
    }

    @Override
    public String toString() {
      return "RecordTime [_call=" + _call + ", _e=" + _e + ", _now=" + _now + ", _timeNs=" + _timeNs + "]";
    }
  }

  private List<RecordTime> _times = new ArrayList<RecordTime>();
  private AtomicInteger _callCount = new AtomicInteger();

  public void recordTime(Enum<?> e, long start, Object... args) {
    _times.add(new RecordTime(e, System.nanoTime(), start, _callCount.incrementAndGet(), args));
  }

  public long startTime() {
    return System.nanoTime();
  }

  public List<RecordTime> getTimes() {
    return _times;
  }

}
