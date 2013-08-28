package org.apache.blur.utils;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;

public class SimpleTimer {

  private Map<String, Long> _startTimes = new HashMap<String, Long>();
  private Map<String, Long> _stopTimes = new HashMap<String, Long>();

  public void start(String name) {
    _startTimes.put(name, System.nanoTime());
  }

  public void stop(String name) {
    _stopTimes.put(name, System.nanoTime());
  }

  public void log(Log log) {
    for (Entry<String, Long> e : _startTimes.entrySet()) {
      long startTime = e.getValue();
      Long stopTime = _stopTimes.get(e.getKey());
      if (stopTime != null) {
        log.info("Timer Name [{0}] took [{1} ms]", e.getKey(), TimeUnit.NANOSECONDS.toMillis(stopTime - startTime));
      } else {
        log.info("Timer Name [{0}] never finished", e.getKey());
      }
    }
  }

}
