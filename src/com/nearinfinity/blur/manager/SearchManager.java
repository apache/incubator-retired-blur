package com.nearinfinity.blur.manager;

import java.util.Map;

import org.apache.lucene.search.Searcher;

public interface SearchManager {

	Map<String,Searcher> getSearcher();

}
