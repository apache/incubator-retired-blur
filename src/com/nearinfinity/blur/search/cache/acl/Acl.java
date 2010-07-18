package com.nearinfinity.blur.search.cache.acl;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.OpenBitSet;

import com.nearinfinity.blur.utils.bitset.UnionDocIdSetIterator;
import com.nearinfinity.blur.utils.bitset.compression.CompressedBitSet;

public class Acl extends Filter {

	private static final long serialVersionUID = -7842735006616114158L;
	private Map<IndexReader, CompressedBitSet[]> acls;
	private String[] names;

	public Acl(Map<IndexReader, CompressedBitSet[]> acls, String... names) {
		this.acls = acls;
		this.names = names;
	}

	@Override
	public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
		final CompressedBitSet[] compressedBitSets = acls.get(reader);
		if (compressedBitSets == null || compressedBitSets.length == 0) {
			return OpenBitSet.EMPTY_DOCIDSET;
		}
		return new DocIdSet() {
			@Override
			public DocIdSetIterator iterator() throws IOException {
				return new UnionDocIdSetIterator(compressedBitSets,names);
			}
		};
	}

}
