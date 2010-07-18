package com.nearinfinity.blur.search.facet;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.search.AbstractCachedQueryManager;
import com.nearinfinity.lucene.util.CompressedBitSet;

public class FacetManager extends AbstractCachedQueryManager<Facet> {
	
	private static final String FACET = "Facet-";

	public FacetManager(String name, boolean auto) {
		super(FACET + name, auto);
	}

	private Map<IndexReader, CompressedBitSet[]> getFacets(String[] names) {
		Map<IndexReader, CompressedBitSet[]> result = new HashMap<IndexReader, CompressedBitSet[]>();
		for (IndexReader indexReader : cachedBitSets.keySet()) {
			CompressedBitSet[] bitSets = new CompressedBitSet[names.length];
			Map<String, CompressedBitSet> map = cachedBitSets.get(indexReader);
			for (int i = 0; i < names.length; i++) {
				CompressedBitSet compressedBitSet = map.get(names[i]);
				if (compressedBitSet == null) {
					bitSets[i] = EMPTY;
				} else {
					bitSets[i] = compressedBitSet;
				}
			}
			result.put(indexReader, bitSets);
		}
		return result;
	}

	@Override
	public Facet create(String... names) {
		return new Facet(getFacets(names),names);
	}
	

}
