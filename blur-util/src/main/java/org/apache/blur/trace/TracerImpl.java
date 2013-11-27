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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.blur.trace.Trace.Parameter;

public class TracerImpl implements Tracer {

  protected final String _name;
  protected final long _start;
  protected long _ended;
  protected final String _threadName;
  protected final long _id;
  protected final Parameter[] _parameters;
  protected final TraceCollector _traceCollector;
  protected final AtomicInteger _scope;
  protected final int _traceScope;

  public TracerImpl(String name, Parameter[] parameters, long id, AtomicInteger scope) {
    _name = name;
    _start = System.nanoTime();
    _threadName = Thread.currentThread().getName();
    _id = id;
    _parameters = parameters;
    _traceCollector = null;
    _scope = scope;
    _traceScope = scope.incrementAndGet();
  }

  public TracerImpl(TraceCollector traceCollector, long id, int traceScope) {
    _name = "new thread collector";
    _start = System.nanoTime();
    _ended = _start;
    _threadName = Thread.currentThread().getName();
    _id = id;
    _parameters = new Parameter[] { new Parameter("requestId", Long.toString(id)) };
    _traceCollector = traceCollector;
    _scope = traceCollector.getScope();
    _traceScope = traceScope;
  }

  @Override
  public void done() {
    _scope.decrementAndGet();
    _ended = System.nanoTime();
  }

  @Override
  public String toString() {
    return "Tracer [name=" + _name + ", id=" + _id + ", thread=" + _threadName + ", started=" + _start + ", took="
        + (_ended - _start) + " ns]";
  }

  public String getName() {
    return _name;
  }

  public long getStart() {
    return _start;
  }

  public long getEnded() {
    return _ended;
  }

  public String getThreadName() {
    return _threadName;
  }

  public String toJson() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("\"id\":").append(_id).append(",");
    builder.append("\"scope\":").append(_traceScope).append(",");
    builder.append("\"name\":\"").append(_name).append("\",");
    builder.append("\"thread\":\"").append(_threadName).append("\",");
    builder.append("\"took\":").append((_ended - _start)).append(",");
    builder.append("\"started\":").append(_start).append(",");
    builder.append("\"ended\":").append(_ended);
    if (_parameters != null) {
      builder.append(",");
      builder.append("\"parameters\":[").append(getParametersJson()).append("]");
    }
    if (_traceCollector != null) {
      builder.append(",");
      builder.append("\"collector\":\n").append(_traceCollector.toJson()).append("");
    }
    builder.append("}");
    return builder.toString();
  }

  private String getParametersJson() {
    StringBuilder builder = new StringBuilder();
    for (Parameter parameter : _parameters) {
      if (builder.length() != 0) {
        builder.append(',');
      }
      builder.append("{\"").append(parameter._name).append("\":\"").append(parameter._value).append("\"}");
    }
    return builder.toString();
  }

}
