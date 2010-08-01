package com.nearinfinity.blur.manager;

import java.util.concurrent.ExecutorService;

import com.nearinfinity.blur.hbase.BlurHits;

public interface SearchExecutor extends UpdatableManager {

	BlurHits search(ExecutorService executor, String query, String filter, long start, int fetchCount);

	long searchFast(ExecutorService executor, String query, String filter, long minimum);

}
