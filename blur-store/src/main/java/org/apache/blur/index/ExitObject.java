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
package org.apache.blur.index;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.utils.ThreadValue;

public class ExitObject {

  private final ThreadValue<AtomicBoolean> _running = new ThreadValue<AtomicBoolean>() {
    @Override
    protected AtomicBoolean initialValue() {
      return new AtomicBoolean(true);
    }
  };

  public AtomicBoolean get() {
    return _running.get();
  }

  public void set(AtomicBoolean running) {
    _running.set(running);
  }

  public void reset() {
    get().set(true);
  }

}
