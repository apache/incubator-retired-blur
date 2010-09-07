package com.nearinfinity.blur.manager;

import java.io.IOException;

import org.apache.lucene.search.Filter;

public class FilterManager {
	
//	private ZooKeeper zk;
//	private IndexManager indexManager;

	public FilterManager(IndexManager indexManager) throws IOException {
//		zk = ZooKeeperFactory.getZooKeeper();
//		this.indexManager = indexManager;
	}

	public Filter getFilter(String table, String name) {
		return null;
	}

}
