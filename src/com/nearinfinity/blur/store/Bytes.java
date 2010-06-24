package com.nearinfinity.blur.store;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class Bytes {

	public static final byte[] EMPTY_BYTES = new byte[]{};
	private static final String UTF_8 = "UTF-8";

	public static byte[] toBytes(String str) {
		try {
			return str.getBytes(UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static long toLong(byte[] b) {
		return ByteBuffer.wrap(b).getLong();
	}

	public static byte[] toBytes(long l) {
		return ByteBuffer.allocate(8).putLong(l).array();
	}

	public static String toString(byte[] str) {
		try {
			return new String(str,UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}
