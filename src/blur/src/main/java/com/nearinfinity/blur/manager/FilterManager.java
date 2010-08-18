package com.nearinfinity.blur.manager;

import java.util.Map;

import org.apache.lucene.search.Filter;

public interface FilterManager {

	Map<String, Filter> getFilters(String table);

}
