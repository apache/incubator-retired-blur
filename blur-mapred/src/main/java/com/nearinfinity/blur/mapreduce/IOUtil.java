package com.nearinfinity.blur.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IOUtil {
    
    private static final String UTF_8 = "UTF-8";

    public static String readString(DataInput input) throws IOException {
        int length = readVInt(input);
        byte[] buffer = new byte[length];
        input.readFully(buffer);
        return new String(buffer,UTF_8);
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
