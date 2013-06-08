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

public class IOUtil {

  private static final String UTF_8 = "UTF-8";

  public static void writeString(DataOutput out, String s) throws IOException {
    if (s == null) {
      out.writeInt(-1);
    }
    byte[] bytes = s.getBytes(UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  public static String readString(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    }
    byte[] bs = new byte[length];
    in.readFully(bs);
    return new String(bs, UTF_8);
  }

}
