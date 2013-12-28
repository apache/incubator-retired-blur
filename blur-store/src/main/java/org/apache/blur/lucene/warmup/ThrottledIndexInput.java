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
package org.apache.blur.lucene.warmup;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * This class has been derived from the
 * org.apache.hadoop.tools.util.ThrottledInputStream class found in Hadoop.
 * 
 * The ThrottledIndexInput provides bandwidth throttling on a specified
 * IndexInput. It is implemented as a wrapper on top of another IndexInput
 * instance. The throttling works by examining the number of bytes read from the
 * underlying IndexInput from the beginning, and sleep()ing for a time interval
 * if the byte-transfer is found exceed the specified tolerable maximum. (Thus,
 * while the read-rate might exceed the maximum for a given short interval, the
 * average tends towards the specified maximum, overall.)
 */
public class ThrottledIndexInput extends IndexInput {

  private static final long SLEEP_DURATION_MS = 1;

  private final IndexInput _rawStream;
  private final double _maxBytesPerSec;
  private final long _startTime;

  private long _bytesRead = 0;
  private long _totalSleepTime = 0;

  public ThrottledIndexInput(IndexInput rawStream, long maxBytesPerSec) {
    super("ThrottledIndexInput(" + rawStream.toString() + ")");
    _rawStream = rawStream;
    _maxBytesPerSec = maxBytesPerSec;
    _startTime = System.nanoTime();
  }

  public ThrottledIndexInput(IndexInput rawStream, long maxBytesPerSec, long bytesRead, long totalSleepTime,
      long startTime) {
    super("ThrottledIndexInput(" + rawStream.toString() + ")");
    _rawStream = rawStream;
    _maxBytesPerSec = maxBytesPerSec;
    _bytesRead = bytesRead;
    _totalSleepTime = totalSleepTime;
    _startTime = startTime;
  }

  /** @inheritDoc */
  @Override
  public long getFilePointer() {
    return _rawStream.getFilePointer();
  }

  /** @inheritDoc */
  @Override
  public void seek(long pos) throws IOException {
    _rawStream.seek(pos);
  }

  /** @inheritDoc */
  @Override
  public long length() {
    return _rawStream.length();
  }

  /** @inheritDoc */
  @Override
  public byte readByte() throws IOException {
    throttle();
    try {
      return _rawStream.readByte();
    } finally {
      _bytesRead++;
    }
  }

  /** @inheritDoc */
  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    throttle();
    try {
      _rawStream.readBytes(b, offset, len);
    } finally {
      _bytesRead += len;
    }
  }

  /** @inheritDoc */
  @Override
  public void close() throws IOException {
    _rawStream.close();
  }

  private void throttle() throws IOException {
    while (getBytesPerSec() > _maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS);
        _totalSleepTime += SLEEP_DURATION_MS;
      } catch (InterruptedException e) {
        throw new IOException("Thread aborted", e);
      }
    }
  }

  /**
   * Getter for the number of bytes read from this stream, since creation.
   * 
   * @return The number of bytes.
   */
  public long getTotalBytesRead() {
    return _bytesRead;
  }

  /**
   * Getter for the read-rate from this stream, since creation. Calculated as
   * bytesRead/elapsedTimeSinceStart.
   * 
   * @return Read rate, in bytes/sec.
   */
  public double getBytesPerSec() {
    double elapsed = (System.nanoTime() - _startTime) / 1000000000;
    if (elapsed == 0) {
      return _bytesRead;
    } else {
      return _bytesRead / elapsed;
    }
  }

  /**
   * Getter the total time spent in sleep.
   * 
   * @return Number of milliseconds spent in sleep.
   */
  public long getTotalSleepTime() {
    return _totalSleepTime;
  }

  /** @inheritDoc */
  @Override
  public String toString() {
    return "ThrottledIndexInput{" + "bytesRead=" + _bytesRead + ", maxBytesPerSec=" + _maxBytesPerSec
        + ", bytesPerSec=" + getBytesPerSec() + ", totalSleepTime=" + _totalSleepTime + '}';
  }

  public long getStartTime() {
    return _startTime;
  }

}