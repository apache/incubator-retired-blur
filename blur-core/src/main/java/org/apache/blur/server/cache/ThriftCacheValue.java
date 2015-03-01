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
package org.apache.blur.server.cache;

import java.util.Arrays;

import org.apache.blur.thirdparty.thrift_0_9_0.TBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryBuffer;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryInputTransport;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.BlurException;

public class ThriftCacheValue<T extends TBase<?, ?>> {

  private final byte[] _data;

  public ThriftCacheValue(T t) throws BlurException {
    _data = toBytes(t);
  }

  public static <T extends TBase<?, ?>> byte[] toBytes(T t) throws BException {
    if (t == null) {
      return null;
    } else {
      TMemoryBuffer transport = new TMemoryBuffer(1024);
      try {
        t.write(new TCompactProtocol(transport));
      } catch (TException e) {
        throw new BException("Unknown error while trying to read from cache.", e);
      }
      return trim(transport.getArray(), transport.length());
    }
  }

  public static byte[] trim(byte[] bs, int len) {
    if (bs.length == len) {
      return bs;
    }
    byte[] buf = new byte[len];
    System.arraycopy(bs, 0, buf, 0, len);
    return buf;
  }

  public int size() {
    if (_data == null) {
      // Cache weight has to have a weight of at least 1.
      return 1;
    }
    return _data.length;
  }

  public T getValue(Class<T> clazz) throws BlurException {
    if (_data == null) {
      return null;
    }
    try {
      T t = clazz.newInstance();
      t.read(new TCompactProtocol(new TMemoryInputTransport(_data)));
      return t;
    } catch (InstantiationException e) {
      throw new BException("Unknown error while trying to read from cache.", e);
    } catch (IllegalAccessException e) {
      throw new BException("Unknown error while trying to read from cache.", e);
    } catch (TException e) {
      throw new BException("Unknown error while trying to read from cache.", e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(_data);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ThriftCacheValue<?> other = (ThriftCacheValue<?>) obj;
    if (!Arrays.equals(_data, other._data))
      return false;
    return true;
  }

}
