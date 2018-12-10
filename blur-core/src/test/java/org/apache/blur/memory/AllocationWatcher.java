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
package org.apache.blur.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.google.monitoring.runtime.instrumentation.Sampler;

public class AllocationWatcher implements MemoryAllocationWatcher, Sampler {

  private static final Log LOG = LogFactory.getLog(AllocationWatcher.class);

  public <T, E extends Exception> T run(Watcher<T, E> w) throws E {
    Thread t = Thread.currentThread();
    Info info = _threadMap.get(t);
    if (info == null) {
      info = new Info();
      info._enabled = true;
      _threadMap.put(t, info);
    }
    return w.run();
  }

  static class Info {
    boolean _enabled;
    Map<String, Long> _count = new HashMap<String, Long>();
    Map<String, Long> _sizes = new HashMap<String, Long>();
    Map<String, Map<StackTraceElement, Long>> _stackElements = new HashMap<String, Map<StackTraceElement, Long>>();
  }

  private final Map<Thread, Info> _threadMap = new ConcurrentHashMap<Thread, Info>();

  @Override
  public void sampleAllocation(int count, String desc, Object newObj, long size) {
    Info info = _threadMap.get(Thread.currentThread());
    if (info != null && info._enabled) {
      StackTraceElement[] stackTrace = new Throwable().getStackTrace();
      StackTraceElement stackTraceElement = stackTrace[2];
      add(desc, info._count, 1L);
      add(desc, info._sizes, size);
      add(desc, info._stackElements, stackTraceElement);
    }
  }

  private void add(String desc, Map<String, Map<StackTraceElement, Long>> stackElements,
      StackTraceElement stackTraceElement) {
    Map<StackTraceElement, Long> map = stackElements.get(desc);
    if (map == null) {
      stackElements.put(desc, map = new HashMap<StackTraceElement, Long>());
    }
    add(stackTraceElement, map, 1L);
  }

  private <K> void add(K key, Map<K, Long> countMap, Long amount) {
    Long c = countMap.get(key);
    if (c == null) {
      countMap.put(key, amount);
    } else {
      countMap.put(key, c + amount);
    }
  }

  public void reset() {
    _threadMap.clear();
  }

  public void dump() {
    Map<Thread, Info> threadMap = _threadMap;
    for (Entry<Thread, Info> entry : threadMap.entrySet()) {
      Thread thread = entry.getKey();
      Info info = entry.getValue();
      Map<String, Long> map = info._count;
      List<Entry<String, Long>> elements = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
      Collections.sort(elements, new Comparator<Entry<String, Long>>() {
        @Override
        public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
          return o1.getValue().compareTo(o2.getValue());
        }
      });
      for (Entry<String, Long> e : elements) {
        String desc = e.getKey();
        LOG.info(thread.getName() + " " + desc + "=>" + e.getValue() + " [" + info._sizes.get(desc) + "]");
        Map<StackTraceElement, Long> stackMap = info._stackElements.get(desc);
        for (Entry<StackTraceElement, Long> stackEntry : stackMap.entrySet()) {
          LOG.info("\t" + stackEntry.getKey() + " " + stackEntry.getValue());
        }
      }
    }
  }

}
