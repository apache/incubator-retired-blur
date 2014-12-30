package org.apache.blur.manager.writer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.hadoop.io.Writable;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class RowMutationWritable implements Writable {

  private RowMutation _rowMutation = new RowMutation();

  public RowMutation getRowMutation() {
    return _rowMutation;
  }

  public void setRowMutation(RowMutation rowMutation) {
    this._rowMutation = rowMutation;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    try {
      _rowMutation.read(getProtocol(in, null));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private TProtocol getProtocol(DataInput in, DataOutput out) {
    return new TCompactProtocol(getTransport(in, out));
  }

  private TTransport getTransport(final DataInput in, final DataOutput out) {
    return new TTransport() {

      @Override
      public void write(byte[] buf, int off, int len) throws TTransportException {
        try {
          out.write(buf, off, len);
        } catch (IOException e) {
          throw new TTransportException(e);
        }
      }

      @Override
      public int read(byte[] buf, int off, int len) throws TTransportException {
        try {
          in.readFully(buf, off, len);
        } catch (IOException e) {
          throw new TTransportException(e);
        }
        return len;
      }

      @Override
      public void open() throws TTransportException {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public boolean isOpen() {
        throw new RuntimeException("Not Implemented");
      }

      @Override
      public void close() {
        throw new RuntimeException("Not Implemented");
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    try {
      _rowMutation.write(getProtocol(null, out));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
