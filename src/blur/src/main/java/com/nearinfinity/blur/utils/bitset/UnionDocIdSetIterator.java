package com.nearinfinity.blur.utils.bitset;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.nearinfinity.blur.utils.bitset.compression.CompressedBitSet;

public class UnionDocIdSetIterator extends DocIdSetIterator {

	public UnionDocIdSetIterator(CompressedBitSet[] compressedBitSets, String[] names) {

	}

	@Override
	public int advance(int target) throws IOException {
		return 0;
	}

	@Override
	public int docID() {
		return 0;
	}

	@Override
	public int nextDoc() throws IOException {
		return 0;
	}

}
