package com.nearinfinity.blur.manager;

import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class SearchManagerImpl implements SearchManager {

	private IndexManager indexManager;
	private volatile Map<String,Searcher> searchers;

	public SearchManagerImpl(IndexManager indexManager) {
		this.indexManager = indexManager;
	}
	
	@Override
	public void update() {
		updateSearchers(indexManager.getCurrentIndexReaders());
	}

	@Override
	public Map<String, Searcher> getSearcher() {
		return searchers;
	}

	private void updateSearchers(Map<String, SuperIndexReader> newIndexReaders) {
		Map<String,Searcher> newSearchers = new TreeMap<String, Searcher>();
		for (Entry<String, SuperIndexReader> entry : newIndexReaders.entrySet()) {
			newSearchers.put(entry.getKey(), new IndexSearcher(entry.getValue()));
		}
		searchers = newSearchers;
	}
}
