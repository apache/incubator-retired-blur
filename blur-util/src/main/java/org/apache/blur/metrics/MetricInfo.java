package org.apache.blur.metrics;

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
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONException;
import org.json.JSONObject;

public class MetricInfo {

  public static void main(String[] args) throws IOException {
    MetricInfo info = new MetricInfo("name", "type", 10);
    info.addNumber("test1", 0.1);
    info.addNumber("test1", 0.2);
    info.addNumber("test1", 0.3);
    info.addNumber("test1", 0.4);
    info.addNumber("test1", 0.5);
    info.addNumber("test1", 0.6);
    info.addNumber("test1", 0.7);
    info.addNumber("test1", 0.9);
    info.addNumber("test1", 1.0);
    info.addNumber("test1", 1.1);
    info.addNumber("test1", 1.2);

    StringWriter writer = new StringWriter();
    info.write(writer);
    System.out.println(writer);
  }

  static interface ArrayWriter {
    void writeArray(Writer writer) throws IOException;
  }

  static class PrimitiveLongQueue implements ArrayWriter {
    int head = -1;
    int length = 0;
    final long[] data;
    final int numberOfElements;

    PrimitiveLongQueue(int numberOfElements) {
      this.data = new long[numberOfElements];
      this.numberOfElements = numberOfElements;
    }

    void add(long d) {
      head++;
      if (head >= numberOfElements) {
        head = 0;
      }
      data[head] = d;
      if (length < numberOfElements) {
        length++;
      }
    }

    public void writeArray(Writer writer) throws IOException {
      writer.append("[");
      int position = head;
      for (int i = 0; i < length; i++) {
        if (i != 0) {
          writer.append(',');
        }
        writer.append(Long.toString(data[position]));
        position--;
        if (position < 0) {
          position = numberOfElements - 1;
        }
      }
      writer.append("]");
    }
  }

  static class PrimitiveDoubleQueue implements ArrayWriter {
    int head = -1;
    int length = 0;
    final double[] data;
    final int numberOfElements;

    PrimitiveDoubleQueue(int numberOfElements) {
      this.data = new double[numberOfElements];
      this.numberOfElements = numberOfElements;
    }

    void add(double d) {
      head++;
      if (head >= numberOfElements) {
        head = 0;
      }
      data[head] = d;
      if (length < numberOfElements) {
        length++;
      }
    }

    public void writeArray(Writer writer) throws IOException {
      writer.append("[");
      int position = head;
      for (int i = 0; i < length; i++) {
        if (i != 0) {
          writer.append(',');
        }
        double d = data[position];
        try {
          writer.append(JSONObject.numberToString(d));
        } catch (JSONException e) {
          throw new IOException(e);
        }
        position--;
        if (position < 0) {
          position = numberOfElements - 1;
        }
      }
      writer.append("]");
    }
  }

  private final String name;
  private final String type;
  private final Map<String, ArrayWriter> metricMap = new ConcurrentHashMap<String, ArrayWriter>();
  private int numberOfEntries;

  public MetricInfo(String name, String type, int numberOfEntries) {
    this.name = name;
    this.type = type;
    this.numberOfEntries = numberOfEntries;
  }

  public void addNumber(String name, long l) {
    PrimitiveLongQueue queue = (PrimitiveLongQueue) metricMap.get(name);
    if (queue == null) {
      queue = new PrimitiveLongQueue(numberOfEntries);
      metricMap.put(name, queue);
    }
    queue.add(l);
  }

  public void addNumber(String name, double d) {
    PrimitiveDoubleQueue queue = (PrimitiveDoubleQueue) metricMap.get(name);
    if (queue == null) {
      queue = new PrimitiveDoubleQueue(numberOfEntries);
      metricMap.put(name, queue);
    }
    queue.add(d);
  }

  public void write(Writer writer) throws IOException {
    writer.append("{\"name\":\"");
    writer.append(name);
    writer.append("\",\"type\":\"");
    writer.append(type);
    writer.append("\",\"data\":{");
    writeMetricMap(writer);
    writer.append("}}");
  }

  private void writeMetricMap(Writer writer) throws IOException {
    boolean flag = false;
    for (Entry<String, ArrayWriter> entry : metricMap.entrySet()) {
      if (flag) {
        writer.append(',');
      }
      writer.append("\"");
      writer.append(entry.getKey());
      writer.append("\":");
      entry.getValue().writeArray(writer);
      flag = true;
    }
  }

  public void setMetaData(String key, String value) {

  }
}
