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
package org.apache.blur.utils;

import java.util.Properties;

public class GCWatcher {

  private static final String JAVA_VERSION = "java.version";
  private static final String _1_7 = "1.7";
  private static final boolean JDK7;

  static {
    Properties properties = System.getProperties();
    String javaVersion = properties.getProperty(JAVA_VERSION);
    if (javaVersion.startsWith(_1_7)) {
      JDK7 = true;
    } else {
      JDK7 = false;
    }
  }

  /**
   * Initializes the GCWatcher to watch for any garbage collection that leaves
   * more then the given ratio free. If more remains then all the given
   * {@link GCAction}s are taken to try and relief the JVM from an
   * {@link OutOfMemoryError} exception.
   * 
   * @param ratio
   *          the ratio of used heap to total heap.
   */
  public static void init(double ratio) {
    if (JDK7) {
      GCWatcherJdk7.init(ratio);
    } else {
      GCWatcherJdk6.init(ratio);
    }
  }

  /**
   * Registers an {@link GCAction} to be taken when the JVM is near an
   * {@link OutOfMemoryError} condition.
   * 
   * @param action
   *          the {@link GCAction}.
   */
  public static void registerAction(GCAction action) {
    if (JDK7) {
      GCWatcherJdk7.registerAction(action);
    } else {
      GCWatcherJdk6.registerAction(action);
    }
  }

  /**
   * Shuts down any internal threads watching the JVM.
   */
  public static void shutdown() {
    if (JDK7) {
      GCWatcherJdk7.shutdown();
    } else {
      GCWatcherJdk6.shutdown();
    }
  }

}
