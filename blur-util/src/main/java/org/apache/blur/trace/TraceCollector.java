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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.trace.Trace.TraceId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class TraceCollector {

  protected final TraceId _id;
  protected final List<TracerImpl> _traces = new CopyOnWriteArrayList<TracerImpl>();
  protected final AtomicLong _traceCounter = new AtomicLong();
  protected final AtomicInteger _scope = new AtomicInteger();
  protected final long _now = (System.currentTimeMillis() * 1000000) + (System.nanoTime() % 1000000);
  protected final long _started = System.nanoTime();
  protected final String _pid;
  protected final String _threadName;
  protected final String _nodeName;
  protected long _finished;

  public TraceCollector(String nodeName, TraceId id) {
    _nodeName = nodeName;
    _id = id;
    _pid = ManagementFactory.getRuntimeMXBean().getName();
    _threadName = Thread.currentThread().getName();
  }

  public TraceCollector(TraceCollector parentCollector, String requestId) {
    _nodeName = parentCollector._nodeName;
    _id = new TraceId(parentCollector._id.getRootId(), requestId);
    _pid = parentCollector._pid;
    _threadName = parentCollector._threadName;
  }

  public void add(TracerImpl tracer) {
    _traces.add(tracer);
  }

  @Override
  public String toString() {
    return "TraceCollector [_id=" + _id + ", _traces=" + _traces + "]";
  }

  public JSONObject toJsonObject() throws JSONException {
    JSONArray tracesArray = new JSONArray();
    for (TracerImpl t : _traces) {
      tracesArray.put(t.toJsonObject());
    }
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("id", _id.toJsonObject());
    jsonObject.put("nodeName", (_nodeName == null ? "unknown" : _nodeName));
    jsonObject.put("pid", _pid);
    jsonObject.put("thread", _threadName);
    jsonObject.put("created", _now);
    jsonObject.put("totalTime", (_finished - _started));
    jsonObject.put("thread", _threadName);
    jsonObject.put("traces", tracesArray);
    return jsonObject;
  }

  public TraceId getId() {
    return _id;
  }

  public List<TracerImpl> getTraces() {
    return _traces;
  }

  public long getNextId() {
    return _traceCounter.incrementAndGet();
  }

  public AtomicInteger getScope() {
    return _scope;
  }

  public void finished() {
    _finished = System.nanoTime();
  }
}
