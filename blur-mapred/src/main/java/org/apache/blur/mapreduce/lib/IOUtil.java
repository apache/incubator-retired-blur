package org.apache.blur.mapreduce.lib;

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

public class IOUtil {

  public static final String UTF_8 = "UTF-8";

  public static String readString(DataInput input) throws IOException {
    int length = readVInt(input);
    byte[] buffer = new byte[length];
    input.readFully(buffer);
    return new String(buffer, UTF_8);
  }

  public static void writeString(DataOutput output, String s) throws IOException {
    byte[] bs = s.getBytes(UTF_8);
    writeVInt(output, bs.length);
    output.write(bs);
  }

  public static int readVInt(DataInput input) throws IOException {
    byte b = input.readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = input.readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
  }

  public static void writeVInt(DataOutput output, int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      output.writeByte((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    output.writeByte((byte) i);
  }

}
