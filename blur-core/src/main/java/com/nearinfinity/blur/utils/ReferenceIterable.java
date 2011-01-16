package com.nearinfinity.blur.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ReferenceIterable<T> implements Iterable<T> {
    
    private T t;

    public ReferenceIterable(T t) {
        this.t = t;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            boolean taken = false;
            @Override
            public boolean hasNext() {
                return !taken;
            }

            @Override
            public T next() {
                if (taken) {
                    throw new NoSuchElementException();
                }
                taken = true;
                return t;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
