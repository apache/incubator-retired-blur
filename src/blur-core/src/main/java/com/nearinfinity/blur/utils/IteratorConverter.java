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
