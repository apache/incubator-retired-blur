package org.apache.blur.utils;

public interface BlurIterable<T, E extends Exception> {

  BlurIterator<T, E> iterator() throws E;

}
