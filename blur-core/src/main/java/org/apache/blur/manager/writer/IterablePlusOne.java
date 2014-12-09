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
package org.apache.blur.manager.writer;

import java.util.Iterator;

public class IterablePlusOne<T> implements Iterable<T> {

  private final T _one;
  private final Iterable<T> _many;

  public IterablePlusOne(T one, Iterable<T> many) {
    _one = one;
    _many = many;
  }

  @Override
  public Iterator<T> iterator() {
    final Iterator<T> iterator = _many.iterator();
    return new Iterator<T>() {

      private boolean _onePickedUp = false;

      @Override
      public boolean hasNext() {
        if (!_onePickedUp) {
          return true;
        }
        return iterator.hasNext();
      }

      @Override
      public T next() {
        if (!_onePickedUp) {
          _onePickedUp = true;
          return _one;
        }
        return iterator.next();
      }

      @Override
      public void remove() {
        throw new RuntimeException("Not Supported.");
      }
    };
  }

}
