package com.nearinfinity.blur.search.facet;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public class FacetCollector extends Collector {

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return false;
	}

	@Override
	public void collect(int doc) throws IOException {
		
	}

	@Override
	public void setNextReader(IndexReader reader, int docBase) throws IOException {
		
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		
	}

}
