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

package com.nearinfinity.blur.utils;

import java.util.Iterator;

public class IteratorConverter<F, T> implements Iterator<T> {
    
    private Converter<F, T> converter;
    private Iterator<F> iterator;

    public IteratorConverter(Iterator<F> iterator, Converter<F, T> converter) {
        this.converter = converter;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        try {
            return converter.convert(iterator.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        iterator.remove();
    }

}
