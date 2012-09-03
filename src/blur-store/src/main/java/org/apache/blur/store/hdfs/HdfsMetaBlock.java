package org.apache.blur.store.hdfs;

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
