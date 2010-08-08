package com.nearinfinity.blur.lucene.store.dao.cassandra.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class Bytes {

	private static final String UTF_8 = "UTF-8";
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};

	public static byte[] toBytes(String s) {
		try {
			return s.getBytes(UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String toString(byte[] bs) {
		try {
			return new String(bs,UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] toBytes(long l) {
		return ByteBuffer.allocate(8).putLong(l).array();
	}

	public static long toLong(byte[] bs) {
		return ByteBuffer.wrap(bs).getLong();
	}
}
