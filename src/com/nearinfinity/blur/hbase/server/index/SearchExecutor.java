package com.nearinfinity.blur.hbase.server.index;

import com.nearinfinity.blur.hbase.BlurHits;

public interface SearchExecutor {

	BlurHits search(String query, String filter, long start, int fetchCount);

	long searchFast(String query, String filter, long minimum);

}
