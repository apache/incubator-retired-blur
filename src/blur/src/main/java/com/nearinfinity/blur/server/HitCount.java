package com.nearinfinity.blur.server;

public class HitCount {
	
	private long totalHits = -1;

	public long getTotalHits() {
		return totalHits;
	}

	public HitCount setTotalHits(long totalHits) {
		this.totalHits = totalHits;
		return this;
	}

}
