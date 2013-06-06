package org.apache.blur.metrics;

import java.io.CharArrayWriter;

public class ResetableCharArrayWriter extends CharArrayWriter {

  public ResetableCharArrayWriter() {
    super();
  }

  public ResetableCharArrayWriter(int initialSize) {
    super(initialSize);
  }

  public char[] getBuffer() {
    return buf;
  }

  public void reset() {
    this.count = 0;
  }

}
