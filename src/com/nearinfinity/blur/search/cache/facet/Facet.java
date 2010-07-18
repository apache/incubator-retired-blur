package com.nearinfinity.blur.search.cache.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;

import com.nearinfinity.blur.utils.bitset.compression.CompressedBitSet;

public class Facet extends Collector {
	
	private String[] names;
	private DocIdSetIterator[] docIdSetIterators;
	private int[] counts;
	private int[] lastDocSeenFromIterator;
	private int length;
	private Map<IndexReader, CompressedBitSet[]> facets;
	private boolean empty = false;

	public Facet(Map<IndexReader, CompressedBitSet[]> facets, final String... names) {
		this.names = names;
		this.counts = new int[names.length];
		this.lastDocSeenFromIterator = new int[names.length];
		this.length = names.length;
		this.facets = facets;
		if (facets.isEmpty()) {
			empty = true;
		}
	}
	
	public Map<String,Integer> getCounts() {
		Map<String,Integer> results = new TreeMap<String, Integer>();
		for (int i = 0; i < length; i++) {
			results.put(names[i], counts[i]);
		}
		return results;
	}
	
	@Override
	public void collect(int doc) throws IOException {
		for (int i = 0; i < length; i++) {
			if (doc == lastDocSeenFromIterator[i]) {
				counts[i]++;
			} else if (doc > lastDocSeenFromIterator[i]) {
				int advance = docIdSetIterators[i].advance(doc);
				if (doc == advance) {
					counts[i]++;
				}
				lastDocSeenFromIterator[i] = advance;
			}
		}
	}
	
	@Override
	public void setNextReader(IndexReader reader, int docBase) throws IOException {
		if (!empty) {
			docIdSetIterators = getIterators(facets.get(reader));
		} else {
			docIdSetIterators = new DocIdSetIterator[length];
			for (int i = 0; i < docIdSetIterators.length; i++) {
				docIdSetIterators[i] = FacetManager.EMPTY.iterator();
			}
		}
		//reset the last doc seen
		Arrays.fill(lastDocSeenFromIterator, -1);
	}
	
	private DocIdSetIterator[] getIterators(CompressedBitSet[] compressedBitSets) throws IOException {
		DocIdSetIterator[] result = new DocIdSetIterator[compressedBitSets.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = compressedBitSets[i].iterator();
		}
		return result;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		//do nothing
	}
	
	@Override
	public boolean acceptsDocsOutOfOrder() {
		return false;
	}
}
