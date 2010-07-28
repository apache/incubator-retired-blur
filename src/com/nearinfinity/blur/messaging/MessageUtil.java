package com.nearinfinity.blur.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.nearinfinity.blur.BlurHit;
import com.nearinfinity.blur.SearchResult;

public class MessageUtil {

	private static final String UTF_8 = "UTF-8";
	
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

	public static byte[] createSearchResults(long totalHits, List<BlurHit> blurHits, byte[]... shardName) throws IOException {
		return createSearchResults(totalHits, blurHits, Arrays.asList(shardName));
	}

	private static void writeHit(DataOutputStream outputStream, BlurHit blurHit) throws IOException {
		outputStream.writeDouble(blurHit.getScore());
		writeString(outputStream,blurHit.getId());
		writeString(outputStream,blurHit.getReason());
	}
	
	private static void writeString(DataOutputStream outputStream, String s) throws IOException {
		byte[] bs = Bytes.toBytes(s);
		outputStream.writeInt(bs.length);
		outputStream.write(bs);
	}

	public static byte[] createSearchResults(long totalHits, List<BlurHit> blurHits, List<byte[]> shardNames) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
		DataOutputStream outputStream = new DataOutputStream(baos);
		outputStream.writeLong(totalHits);
		if (blurHits != null) {
			outputStream.writeLong(blurHits.size());
			for (int i = 0; i < blurHits.size(); i++) {
				BlurHit blurHit = blurHits.get(i);
				writeHit(outputStream,blurHit);
			}
		} else {
			outputStream.writeLong(-1);
		}
		int size = shardNames.size();
		outputStream.write(size);
		for (int i = 0; i < size; i++) {
			byte[] bs = shardNames.get(i);
			outputStream.writeInt(bs.length);
			outputStream.write(bs);
		}
		outputStream.close();
		return baos.toByteArray();
	}
	
	public static SearchResult getSearchResult(byte[] search) throws IOException {
		SearchResult result = new SearchResult();
		DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(search));
		result.count = inputStream.readLong();
		long blurHitCount = inputStream.readLong();
		result.hits = new ArrayList<BlurHit>();
		for (int i = 0; i < blurHitCount; i++) {
			SimpleBlurHit blurHit = new SimpleBlurHit();
			blurHit.setScore(inputStream.readDouble());
			blurHit.setId(readString(inputStream));
			blurHit.setReason(readString(inputStream));
			result.hits.add(blurHit);
		}
		result.respondingShards = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		return result;
	}

	private static String readString(DataInputStream inputStream) throws IOException {
		int length = inputStream.readInt();
		byte[] buffer = new byte[length];
		inputStream.readFully(buffer);
		return Bytes.toString(buffer);
	}
	
}
