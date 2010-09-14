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
