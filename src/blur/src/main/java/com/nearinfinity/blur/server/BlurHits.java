package com.nearinfinity.blur.server;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class BlurHits {
	
	public static class BlurHit implements Comparable<BlurHit> {
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
		public String toString() {
			return score + "," + id + "," + reason;
		}
	}
	
	private long totalHits = 0;
	private Set<BlurHit> hits = new TreeSet<BlurHit>();
	private Map<String,Integer> hitsPerShard = new TreeMap<String, Integer>();
	
	public void setHits(String shardId, int hitCount) {
		hitsPerShard.put(shardId, hitCount);
	}
	
	public Map<String, Integer> getHitsPerShard() {
		return hitsPerShard;
	}

	public void setHitsPerShard(Map<String, Integer> hitsPerShard) {
		this.hitsPerShard = hitsPerShard;
	}

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

	public synchronized void merge(BlurHits blurHits) {
		totalHits += blurHits.totalHits;
		hitsPerShard.putAll(blurHits.hitsPerShard);
		hits.addAll(blurHits.hits);
	}

	public void add(BlurHit blurHit) {
		hits.add(blurHit);
	}

	@Override
	public String toString() {
		return "totalHits:" + totalHits + "," + hits.toString();
	}

	public BlurHits reduceHitsTo(int fetchCount) {
		hits = reduce(hits, fetchCount);
		return this;
	}
	
	private Set<BlurHit> reduce(Set<BlurHit> hits, int fetchCount) {
		Set<BlurHit> topHits = new TreeSet<BlurHit>();
		for (BlurHit blurHit : hits) {
			topHits.add(blurHit);
			if (topHits.size() >= fetchCount) {
				return topHits;
			}
		}
		return topHits;
	}

}
