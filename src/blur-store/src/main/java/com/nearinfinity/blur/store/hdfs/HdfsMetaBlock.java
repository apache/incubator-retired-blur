package com.nearinfinity.blur.store.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class HdfsMetaBlock implements Writable {
  long logicalPosition;
  long realPosition;
  long length;

  @Override
  public void readFields(DataInput in) throws IOException {
    logicalPosition = in.readLong();
    realPosition = in.readLong();
    length = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(logicalPosition);
    out.writeLong(realPosition);
    out.writeLong(length);
  }

  boolean containsDataAt(long logicalPos) {
    if (logicalPos >= logicalPosition && logicalPos < logicalPosition + length) {
      return true;
    }
    return false;
  }

  long getRealPosition(long logicalPos) {
    long offset = logicalPos - logicalPosition;
    long pos = realPosition + offset;
    return pos;
  }

  @Override
  public String toString() {
    return "HdfsMetaBlock [length=" + length + ", logicalPosition=" + logicalPosition + ", realPosition=" + realPosition + "]";
  }
}
