package com.nearinfinity.blur.messaging;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.nearinfinity.blur.SearchResult;

public class MessageUtil {

	private static final String UTF_8 = "UTF-8";

	public static SearchResult getSearchResult(byte[] search) {
		SearchResult result = new SearchResult();
		ByteBuffer buffer = ByteBuffer.wrap(search);
		result.count = buffer.getLong();
		return result;
	}
	
	public static byte[] getSearchFastMessage(String query, String filter, long minimum) {
		byte[] q = toBytes(query);
		byte[] f = toBytes(filter);
		return ByteBuffer.allocate(q.length + f.length + 8 + 8 + 1).
			put((byte) 1).
			putLong(minimum).
			putInt(q.length).
			put(q).
			putInt(f.length).
			put(f).
			array();
	}

	public static byte[] getSearchMessage(String query, String filter, long starting, int fetch) {
		byte[] q = toBytes(query);
		byte[] f = toBytes(filter);
		return ByteBuffer.allocate(q.length + f.length + 8 + 12 + 1).
			put((byte) 2).
			putLong(starting).
			putInt(fetch).
			putInt(q.length).
			put(q).
			putInt(f.length).
			put(f).
			array();
	}

	public static SearchMessage getSearchMessage(byte[] message) {
		ByteBuffer buffer = ByteBuffer.wrap(message);
		byte type = buffer.get();
		SearchMessage searchMessage = new SearchMessage();
		if (type == 1) {
			searchMessage.minimum = buffer.getLong();
			searchMessage.query = getString(buffer);
			searchMessage.filter = getString(buffer);
			searchMessage.fastSearch = true;
		} else if (type == 2) {
			searchMessage.starting = buffer.getLong();
			searchMessage.fetch = buffer.getInt();
			searchMessage.query = getString(buffer);
			searchMessage.filter = getString(buffer);
			searchMessage.fastSearch = false;
		} else {
			throw new RuntimeException("Type unknown.");
		}
		return searchMessage;
	}
	
	public static String getString(ByteBuffer buffer) {
		int length = buffer.getInt();
		byte[] buf = new byte[length];
		buffer.get(buf);
		try {
			return new String(buf,UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] toBytes(String s) {
		try {
			return s.getBytes(UTF_8);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
