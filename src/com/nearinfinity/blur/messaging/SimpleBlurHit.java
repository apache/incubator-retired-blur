package com.nearinfinity.blur.messaging;

import com.nearinfinity.blur.BlurHit;

public class SimpleBlurHit implements BlurHit {

	private String id;
	private String reason;
	private double score;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

}
