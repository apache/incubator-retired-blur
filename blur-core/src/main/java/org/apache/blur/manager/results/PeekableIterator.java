package org.apache.blur.manager.results;

import org.apache.blur.utils.BlurIterator;


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

public class PeekableIterator<T, E extends Exception> implements BlurIterator<T, E> {

  private final BlurIterator<T, E> _iterator;
  private T _current;
  
  private PeekableIterator(BlurIterator<T, E> iterator,T current) {
    _iterator = iterator;
    _current = current;
  }
  
  public static<T,E extends Exception> PeekableIterator<T, E> wrap(BlurIterator<T, E> iterator) throws E {
    if (iterator.hasNext()) {
      return new PeekableIterator<T, E>(iterator, iterator.next());
    }
    return new PeekableIterator<T, E>(iterator, null);
  }

  /**
   * Only valid is hasNext is true. If hasNext if false, peek will return null;
   * 
   * @return <T>
   */
  public T peek() {
    return _current;
  }

  @Override
  public boolean hasNext() throws E {
    if (_current != null) {
      return true;
    }
    return _iterator.hasNext();
  }

  @Override
  public T next() throws E {
    T next = null;
    if (_iterator.hasNext()) {
      next = _iterator.next();
    }
    T result = _current;
    _current = next;
    return result;
  }

}
