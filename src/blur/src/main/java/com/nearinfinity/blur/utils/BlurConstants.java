package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.server.BlurHits;

public interface BlurConstants {
	public static final BlurHits EMTPY_HITS = new BlurHits();
	public static final String BLUR_DATA_STORAGE_STORE_CLASS = "blur.data.storage.store.class";
	public static final String BLUR_DIRECTORY_MANAGER_STORE_CLASS = "blur.directory.manager.store.class";
	public static final String BLUR_ZOOKEEPER_PATH = "blur.zookeeper.path";
	public static final String BLUR_SHARDS_TOSERVE_PER_PASS = "blur.shards.toserve.per.pass";
	
	
	
	
	public static final String SEARCH_START = "s";
	public static final String SEARCH_FETCH_COUNT = "c";
	public static final String SEARCH_FAST = "fast";
	public static final String SEARCH_QUERY = "q";
	public static final String SEARCH_FILTER = "f";
	public static final String SEARCH_MINIMUM = "m";
}
