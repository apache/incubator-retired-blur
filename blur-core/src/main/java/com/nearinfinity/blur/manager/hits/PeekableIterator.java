/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.hits;

import java.util.Iterator;

public class PeekableIterator<E> implements Iterator<E> {
    
    private Iterator<E> iterator;
    private E current;

    public PeekableIterator(Iterator<E> iterator) {
        if (iterator.hasNext()) {
            current = iterator.next();
        }
        this.iterator = iterator;
    }
    
    /**
     * Only valid is hasNext is true.  If hasNext if false, peek will return null;
     * @return <E>
     */
    public E peek() {
        return current;
    }

    @Override
    public boolean hasNext() {
        if (current != null) {
            return true;
        }
        return iterator.hasNext();
    }

    @Override
    public E next() {
        E next = null;
        if (iterator.hasNext()) {
            next = iterator.next();
        }
        E result = current;
        current = next;
        return result;
    }

    @Override
    public void remove() {
        
    }

}
