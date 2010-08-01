package com.nearinfinity.blur.hbase.server.index;

import java.util.Map;

import org.apache.lucene.search.Searcher;

public interface SearchManager {

	Map<String,Searcher> getSearcher();

}
