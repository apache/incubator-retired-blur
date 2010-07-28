package com.nearinfinity.blur.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtils {

	public static String readerString(DataInput input) throws IOException {
		int length = input.readInt();
		byte[] buffer = new byte[length];
		input.readFully(buffer);
		return Bytes.toString(buffer);
	}

	public static void writeString(String str, DataOutput output) throws IOException {
		byte[] bs = Bytes.toBytes(str);
		output.writeInt(bs.length);
		output.write(bs);
	}

}
