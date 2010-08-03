package com.nearinfinity.blur.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import com.nearinfinity.blur.utils.HbaseUtils;

public class BlurHits implements Writable {
	
	public static class BlurHit implements Comparable<BlurHit>, Writable {
		public String id;
		public String reason;
		public double score;
		
		public BlurHit(double score, String id, String reason) {
			this.score = score;
			this.id = id;
			this.reason = reason;
		}

		public BlurHit() {

		}

		@Override
		public int compareTo(BlurHit blurHit) {
			int scoreCompare = Double.compare(score, blurHit.score);
			if (scoreCompare == 0) {
				return id.compareTo(blurHit.id);
			}
			return scoreCompare;
		}
		
		@Override
		public void readFields(DataInput input) throws IOException {
			score = input.readDouble();
			id = HbaseUtils.readerString(input);
			reason = HbaseUtils.readerString(input);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeDouble(score);
			HbaseUtils.writeString(id,output);
			HbaseUtils.writeString(reason,output);
		}

		@Override
		public String toString() {
			return score + "," + id + "," + reason;
		}

		public void toJson(PrintWriter printWriter) {
			printWriter.println("{\"score\":" + score + ",\"id\":\"" + id +	"\",\"reason\":\"" + reason + "\"}");
		}
	}
	
	private long totalHits = 0;
	private Set<BlurHit> hits = new TreeSet<BlurHit>();
	
	public long getTotalHits() {
		return totalHits;
	}

	public void setTotalHits(long totalHits) {
		this.totalHits = totalHits;
	}

	public Set<BlurHit> getHits() {
		return hits;
	}

	public void setHits(Set<BlurHit> hits) {
		this.hits = hits;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		totalHits = input.readLong();
		int count = input.readInt();
		for (int i = 0; i < count; i++) {
			BlurHit blurHit = new BlurHit();
			blurHit.readFields(input);
			hits.add(blurHit);
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(totalHits);
		output.writeInt(hits.size());
		for (BlurHit blurHit : hits) {
			blurHit.write(output);
		}
	}

	public synchronized void merge(BlurHits blurHits) {
		totalHits += blurHits.totalHits;
		hits.addAll(blurHits.hits);
	}

	public void add(BlurHit blurHit) {
		hits.add(blurHit);
	}

	@Override
	public String toString() {
		return "totalHits:" + totalHits + "," + hits.toString();
	}

	public void toJson(PrintWriter printWriter) {
		printWriter.println("{\"totalHits\":" + totalHits + ",\"hits\":[");
		boolean flag = true;
		for (BlurHit hit : hits) {
			if (!flag) {
				printWriter.println(',');
			}
			hit.toJson(printWriter);
			flag = false;
		}
		printWriter.println("]}");
	}
}
