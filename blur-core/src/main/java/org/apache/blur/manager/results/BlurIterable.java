package org.apache.blur.manager.results;

public interface BlurIterable<T, E extends Exception> {

  BlurIterator<T, E> iterator() throws E;

}
