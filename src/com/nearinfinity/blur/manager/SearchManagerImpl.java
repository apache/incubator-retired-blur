package com.nearinfinity.blur.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;

public class SearchManagerImpl implements SearchManager {

	private static final long WAIT_BETWEEN_PASSES = 10000;
	private IndexManager indexManager;
	private Timer timer;
	private Map<String,Searcher> searchers;

	public SearchManagerImpl(IndexManager indexManager) {
		this.indexManager = indexManager;
		this.timer = new Timer("Searcher-Refresher",true);
		this.timer.scheduleAtFixedRate(getTask(), WAIT_BETWEEN_PASSES, WAIT_BETWEEN_PASSES);
	}

	@Override
	public Map<String, Searcher> getSearcher() {
		return searchers;
	}
	
	private TimerTask getTask() {
		return new TimerTask() {
			@Override
			public void run() {
				updateSearchers(indexManager.getCurrentIndexReaders());
			}
		};
	}
	
	private void updateSearchers(Map<String, IndexReader> newIndexReaders) {
		Map<String,Searcher> newSearchers = new HashMap<String, Searcher>();
		for (Entry<String, IndexReader> entry : newIndexReaders.entrySet()) {
			newSearchers.put(entry.getKey(), new IndexSearcher(entry.getValue()));
		}
		searchers = newSearchers;
	}
}
