package com.nearinfinity.blur.messaging;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BlurMessage {
	
	private static final String UTF_8 = "UTF-8";

	public class Entry {
		float score;
		String id;
		String reason;
	}
	
	private float previousScore = Float.MAX_VALUE;
	private ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
	private DataOutputStream outputStream = new DataOutputStream(baos);
	
	public void setHits(long hits) throws IOException {
		outputStream.writeLong(hits);
	}
	
	public void addNextEntry(float score, String id, String reason) throws IOException {
		if (score > previousScore) {
			throw new IllegalArgumentException("Entries have to be pre sorted from lucene.");
		}
		byte[] idBs = id.getBytes(UTF_8);
		byte[] reasonBs = reason.getBytes(UTF_8);
		outputStream.writeFloat(score);
		outputStream.writeInt(idBs.length);
		outputStream.write(idBs);
		outputStream.writeInt(reasonBs.length);
		outputStream.write(reasonBs);
	}
	
}
