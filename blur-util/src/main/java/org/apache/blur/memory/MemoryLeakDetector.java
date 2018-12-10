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
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.google.common.collect.MapMaker;

public class MemoryLeakDetector {

  private static final Log LOG = LogFactory.getLog(MemoryLeakDetector.class);
  
  private static boolean _enabled = false;
  private static final ConcurrentMap<Object, Info> _map;
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

  static {
    _map = new MapMaker().weakKeys().makeMap();
    _timer = new Timer("MemoryLeakDetector",true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        dump();
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
    for (Entry<Object, Info> e : entrySet) {
      Object o = e.getKey();
      if (o != null) {
        Info info = e.getValue();
        LOG.info("Object [{0}] Info [{1}]", o, info);
      }
    }
  }

}
