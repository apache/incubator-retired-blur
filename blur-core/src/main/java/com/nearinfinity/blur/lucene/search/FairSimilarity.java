package com.nearinfinity.blur.lucene.search;

import org.apache.lucene.search.Similarity;

public class FairSimilarity extends Similarity {

	private static final long serialVersionUID = 8819964136561756067L;

	@Override
	public float coord(int overlap, int maxOverlap) {
		return 1;
	}

	@Override
	public float idf(int docFreq, int numDocs) {
		return 1;
	}

	@Override
	public float lengthNorm(String fieldName, int numTokens) {
		return 1;
	}

	@Override
	public float queryNorm(float sumOfSquaredWeights) {
		return 1;
	}

	@Override
	public float sloppyFreq(int distance) {
		return 1;
	}

	@Override
	public float tf(float freq) {
		return 1;
	}

}
