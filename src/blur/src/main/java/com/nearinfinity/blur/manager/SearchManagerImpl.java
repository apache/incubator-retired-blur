package com.nearinfinity.blur.manager;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;

public class SearchManagerImpl implements SearchManager, BlurConstants {

	private final static Logger LOG = LoggerFactory.getLogger(SearchManagerImpl.class);
	private IndexManager indexManager;
	private volatile Map<String,Map<String,Searcher>> searchers = new TreeMap<String, Map<String,Searcher>>();
	private Similarity similarity;
	private BlurConfiguration configuration = new BlurConfiguration();

	public SearchManagerImpl(IndexManager indexManager) {
		this.indexManager = indexManager;
		this.similarity = configuration.getNewInstance(BLUR_LUCENE_SIMILARITY_CLASS, Similarity.class);
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
				IndexSearcher searcher = new IndexSearcher(entry.getValue());
				searcher.setSimilarity(similarity);
				newSearcherMap.put(entry.getKey(), searcher);
			}
			newSearchers.put(table, newSearcherMap);
		}
		LOG.info("New Searchers {}",newSearchers);
		searchers = newSearchers;
	}


}
