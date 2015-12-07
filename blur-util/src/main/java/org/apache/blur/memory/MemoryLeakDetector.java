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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class MemoryLeakDetector {

  private static final Log LOG = LogFactory.getLog(MemoryLeakDetector.class);

  private static boolean _enabled = false;
  private static final Map<Object, Info> _map;
  private static Timer _timer;

  static class Info {
    final Object[] _args;
    final String _message;

    Info(String message, Object[] args) {
      _args = args;
      _message = message;
    }

    @Override
    public String toString() {
      return "Info [_args=" + Arrays.toString(_args) + ", _message=" + _message + "]";
    }

  }

  public static boolean isEnabled() {
    return _enabled;
  }

  public static void setEnabled(boolean enabled) {
    MemoryLeakDetector._enabled = enabled;
  }

  static {
    _map = Collections.synchronizedMap(new WeakHashMap<Object, Info>());
    _timer = new Timer("MemoryLeakDetector", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          dump();
        } catch (Throwable t) {
          LOG.error("Unknown error.", t);
        }
      }
    }, TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10));
  }

  public static <T> T record(T t, String message, Object... args) {
    if (_enabled) {
      String realMessage = MessageFormat.format(message, args);
      Info info = new Info(realMessage, args);
      _map.put(t, info);
    }
    return t;
  }

  public static void dump() {
    Set<Entry<Object, Info>> entrySet = _map.entrySet();
    int count = 0;
    for (Entry<Object, Info> e : entrySet) {
      Object o = e.getKey();
      if (o != null) {
        Info info = e.getValue();
        LOG.info("Id [{0}] Hashcode [{1}] Object [{2}] Info [{3}]", count, System.identityHashCode(o), o, info);
        count++;
      }
    }
  }

  public static boolean isEmpty() {
    return _map.isEmpty();
  }

  public static int getCount() {
    return _map.size();
  }

}
