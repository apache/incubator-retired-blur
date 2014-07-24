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
package org.apache.blur.server.platform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;
import org.apache.blur.thrift.generated.ValueType;

public class CommandUtils {

  public static byte[] toBytesViaSerialization(Object object) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream outputStream = new ObjectOutputStream(out);
    outputStream.writeObject(object);
    outputStream.close();
    return out.toByteArray();
  }

  public static Value toValue(Object object) throws IOException {
    Value value = new Value();
    value.setType(ValueType.SERIALIZABLE);
    value.setValue(toBytesViaSerialization(object));
    return value;
  }

  @SuppressWarnings("unchecked")
  public static <T> T toObjectViaSerialization(ClassLoader classLoader, byte[] instanceData) throws IOException {
    CommandObjectInputStream inputStream = new CommandObjectInputStream(classLoader, instanceData);
    try {
      return (T) inputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      inputStream.close();
    }
  }

  public static <T> T toObject(ClassLoader classLoader, Value value) throws BlurException, IOException {
    ValueType type = value.getType();
    switch (type) {
    case SERIALIZABLE:
      return toObjectViaSerialization(classLoader, value.getValue());
    default:
      throw new BException("Type [{0}] not supported.", type);
    }
  }

  public static class CommandObjectInputStream extends ObjectInputStream {

    private final ClassLoader _loader;

    public CommandObjectInputStream(ClassLoader loader, InputStream in) throws IOException {
      super(in);
      _loader = loader;
    }

    public CommandObjectInputStream(ClassLoader classLoader, byte[] bs) throws IOException {
      this(classLoader, new ByteArrayInputStream(bs));
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      String name = desc.getName();
      try {
        return Class.forName(name, false, _loader);
      } catch (ClassNotFoundException ex) {
        return super.resolveClass(desc);
      }
    }
  }

}
