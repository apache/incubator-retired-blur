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
package org.apache.blur.thrift;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ClientLock extends ReentrantLock {

  private static final long serialVersionUID = -3246136431587733012L;
  
  private final String _desc;

  public ClientLock(String desc) {
    _desc = desc;
  }
  
  public void errorFailLock() {
    if (tryLock()) {
      return;
    }
    Thread thread = getOwner();
    throw new RuntimeException("Thread [" + thread + "] owns this " + _desc + "."
        + "Including the current stacktrace of the owner (ENDING with <<<<<<):\n\n" + getStackTrace(thread)
        + "\n<<<<<<\n");
  }

  private String getStackTrace(Thread thread) {
    Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
    StackTraceElement[] stackTraceElements = allStackTraces.get(thread);
    if (stackTraceElements != null) {
      StringBuilder builder = new StringBuilder();
      for (StackTraceElement stackTraceElement : stackTraceElements) {
        builder.append("\tOwner at ").append(stackTraceElement.toString()).append("\n");
      }
      return builder.toString();
    }
    return "Unknown";
  }
}