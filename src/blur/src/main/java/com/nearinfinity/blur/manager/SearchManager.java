package com.nearinfinity.blur.manager;

import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Searcher;

public interface SearchManager extends UpdatableManager {

	Map<String,Searcher> getSearchers(String table);
	Set<String> getTables();

}
