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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.utils.GCWatcher.Action;

public class CreateGarbage {

  public static void main(String[] args) throws InterruptedException {
    final Map<String, String> map = new ConcurrentHashMap<String, String>();
    Action action = new Action() {
      @Override
      public void takeAction() throws Exception {
        map.clear();
      }
    };
    GCWatcher.init(0.75);
    GCWatcher.registerAction(action);
    while (true) {

      int count = 0;
      int max = 100000;
      for (long i = 0; i < 100000000; i++) {
        if (count >= max) {
          Thread.sleep(250);
          count = 0;
        }
        map.put(Long.toString(i), Long.toString(i));
        count++;
      }
      System.out.println(map.size());
      map.clear();
    }
  }

}
