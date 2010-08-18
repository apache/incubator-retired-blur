package com.nearinfinity.blur.manager;

import org.apache.lucene.search.Filter;

public interface FilterManager {

	Filter getFilter(String table, String name);

}
