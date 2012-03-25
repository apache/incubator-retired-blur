package org.apache.lucene.index;

import java.util.concurrent.atomic.AtomicBoolean;

public interface WarmUpByFieldBoundsStatus {

  void complete(String name, Term start, Term end, long startPosition, long endPosition, long totalBytesRead, long nanoTime, AtomicBoolean isClosed);

}
