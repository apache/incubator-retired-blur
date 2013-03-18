package org.apache.blur.lucene.serializer;

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
