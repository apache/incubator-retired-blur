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
 * See the =License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.utils;

import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

public class ThreadValue<T> {

  static class Value<T> {
    T value;

    Value(T value) {
      this.value = value;
    }
  }

  private final ConcurrentMap<Thread, Value<T>> refs = new MapMaker().weakKeys().makeMap();

  protected T initialValue() {
    return null;
  }

  public T get() {
    Value<T> value = refs.get(Thread.currentThread());
    if (value == null) {
      refs.put(Thread.currentThread(), value = new Value<>(initialValue()));
    }
    return value.value;
  }

  public void set(T v) {
    Value<T> value = refs.get(Thread.currentThread());
    if (value == null) {
      refs.put(Thread.currentThread(), value = new Value<>(v));
    } else {
      value.value = v;
    }
  }

}
