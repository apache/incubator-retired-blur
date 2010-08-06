package com.nearinfinity.blur.manager;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.nearinfinity.blur.server.BlurHits;

public interface SearchExecutor extends UpdatableManager {
	
	Set<String> getTables();
	BlurHits search(ExecutorService executor, String table, String query, String filter, long start, int fetchCount);
	long searchFast(ExecutorService executor, String table, String query, String filter, long minimum);

}
