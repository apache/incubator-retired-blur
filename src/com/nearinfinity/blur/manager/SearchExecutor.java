package com.nearinfinity.blur.manager;

import com.nearinfinity.blur.hbase.BlurHits;

public interface SearchExecutor {

	BlurHits search(String query, String filter, long start, int fetchCount);

	long searchFast(String query, String filter, long minimum);

}
