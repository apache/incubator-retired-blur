package com.nearinfinity.blur.manager;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class SearchManagerImpl implements SearchManager {

	private static final Log LOG = LogFactory.getLog(SearchManagerImpl.class);
	private IndexReaderManager indexManager;
	private volatile Map<String,Map<String,Searcher>> searchers = new TreeMap<String, Map<String,Searcher>>();

	public SearchManagerImpl(IndexReaderManager indexManager) {
		this.indexManager = indexManager;
	}
	
	@Override
	public void update() {
		updateSearchers(indexManager.getCurrentIndexReaders());
	}

	@Override
	public Map<String, Searcher> getSearchers(String table) {
		return searchers.get(table);
	}
	
	@Override
	public Set<String> getTables() {
		return new TreeSet<String>(searchers.keySet());
	}

	private void updateSearchers(Map<String,Map<String, SuperIndexReader>> newIndexReaders) {
		Map<String,Map<String,Searcher>> newSearchers = new TreeMap<String, Map<String,Searcher>>();
		for (String table : newIndexReaders.keySet()) {
			Map<String, SuperIndexReader> readers = newIndexReaders.get(table);
			Map<String, Searcher> newSearcherMap = new TreeMap<String, Searcher>();
			for (Entry<String, SuperIndexReader> entry : readers.entrySet()) {
				newSearcherMap.put(entry.getKey(), new IndexSearcher(entry.getValue()));
			}
			newSearchers.put(table, newSearcherMap);
		}
		LOG.info("New Searchers [" + newSearchers + "]");
		searchers = newSearchers;
	}


}
