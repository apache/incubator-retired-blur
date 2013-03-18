package org.apache.blur.lucene.serializer;

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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

public class SerializerUtil {

  public static void writeString(String s, DataOutput out) throws IOException {
    BytesRef bytes = new BytesRef();
    UnicodeUtil.UTF16toUTF8(s, 0, s.length(), bytes);
    writeBytesRef(bytes, out);
  }

  public static void writeBytesRef(BytesRef bytes, DataOutput out) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes.bytes, bytes.offset, bytes.length);
  }

  public static String readString(DataInput in) throws IOException {
    BytesRef bytes = readBytesRef(in);
    return bytes.utf8ToString();
  }

  public static BytesRef readBytesRef(DataInput in) throws IOException {
    int length = in.readInt();
    BytesRef bytes = new BytesRef(length);
    in.readFully(bytes.bytes);
    bytes.offset = 0;
    bytes.length = length;
    return bytes;
  }

}
