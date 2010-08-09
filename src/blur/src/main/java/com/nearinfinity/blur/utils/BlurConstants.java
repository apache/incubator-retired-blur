package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.server.BlurHits;

public interface BlurConstants {
	public static final BlurHits EMTPY_HITS = new BlurHits();
	public static final String BLUR_MASTER_PORT = "blur.master.port";
	public static final String BLUR_NODE_PORT = "blur.node.port";
	public static final String BLUR_DATA_STORAGE_STORE_CLASS = "blur.data.storage.store.class";
	public static final String BLUR_DIRECTORY_MANAGER_STORE_CLASS = "blur.directory.manager.store.class";
	public static final String BLUR_ZOOKEEPER_PATH = "blur.zookeeper.path";
	public static final String BLUR_SHARDS_TOSERVE_PER_PASS = "blur.shards.toserve.per.pass";
	public static final String BLUR_LUCENE_SIMILARITY_CLASS = "blur.lucene.similarity.class";
	public static final String BLUR_INDEXING_WAIT_TIME = "blur.indexing.wait.time";
	public static final String BLUR_DATATOSUPERDOCUMENT_CONVERTER_CLASS = "blur.datatosuperdocument.converter.class";
	public static final String BLUR_TABLEANALYZER_MANAGER_CLASS = "blur.tableanalyzer.manager.class";
	
	
	public static final String SEARCH_START = "s";
	public static final String SEARCH_FETCH_COUNT = "c";
	public static final String SEARCH_FAST = "fast";
	public static final String SEARCH_QUERY = "q";
	public static final String SEARCH_ACL = "a";
	public static final String SEARCH_MINIMUM = "m";
}
