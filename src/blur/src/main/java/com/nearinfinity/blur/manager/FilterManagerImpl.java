package com.nearinfinity.blur.manager;

import java.io.IOException;

import org.apache.lucene.search.Filter;

public class FilterManagerImpl {
	
//	private ZooKeeper zk;
//	private IndexManager indexManager;

	public FilterManagerImpl(IndexManagerImpl indexManager) throws IOException {
//		zk = ZooKeeperFactory.getZooKeeper();
//		this.indexManager = indexManager;
	}

	public Filter getFilter(String table, String name) {
		return null;
	}

}
