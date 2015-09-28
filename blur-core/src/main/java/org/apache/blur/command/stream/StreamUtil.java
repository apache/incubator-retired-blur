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
package org.apache.blur.command.stream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.output.ByteArrayOutputStream;

public class StreamUtil {

  private static final String UTF_8 = "UTF-8";

  public static void writeString(DataOutput output, String s) throws IOException {
    try {
      byte[] bs = s.getBytes(UTF_8);
      output.writeInt(bs.length);
      output.write(bs);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Does not supported UTF-8?", e);
    }
  }

  public static String readString(DataInput input) throws IOException {
    int length = input.readInt();
    byte[] buf = new byte[length];
    input.readFully(buf);
    return new String(buf, UTF_8);
  }

  public static byte[] toBytes(Serializable s) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(outputStream);
    out.writeObject(s);
    out.close();
    return outputStream.toByteArray();
  }
}
