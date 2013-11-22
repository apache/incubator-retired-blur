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

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class TraceCollector {

  protected final String _id;
  protected final List<TracerImpl> _traces;
  protected final AtomicLong _traceCounter;
  protected final long _now = System.nanoTime();
  protected final String _pid;

  public TraceCollector(String id) {
    _id = id;
    _traces = new CopyOnWriteArrayList<TracerImpl>();
    _traceCounter = new AtomicLong();
    _pid = ManagementFactory.getRuntimeMXBean().getName();
  }

  public void add(TracerImpl tracer) {
    _traces.add(tracer);
  }

  @Override
  public String toString() {
    return "TraceCollector [_id=" + _id + ", _traces=" + _traces + "]";
  }

  public String toJson() {
    StringBuilder builder = new StringBuilder();
    for (TracerImpl t : _traces) {
      builder.append("    ").append(t.toJson()).append(",\n");

    }

    return "{\n  \"id\"=\"" + _id + "\"\n  \"pid\"=\"" + _pid + "\"\n  \"created\"=" + _now + ",\n  \"traces\"=[\n" + builder.toString() + "  ]\n}";
  }

  public String getId() {
    return _id;
  }

  public List<TracerImpl> getTraces() {
    return _traces;
  }

  public long getNextId() {
    return _traceCounter.incrementAndGet();
  }
}
