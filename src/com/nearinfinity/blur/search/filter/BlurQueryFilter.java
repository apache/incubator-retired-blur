package com.nearinfinity.blur.search.filter;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

public class BlurQueryFilter extends Filter {

	private static final long serialVersionUID = 7545974090676284603L;

	@Override
	public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
		return null;
	}

}
