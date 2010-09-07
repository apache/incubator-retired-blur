package com.nearinfinity.blur.manager;

import java.io.IOException;

import org.apache.lucene.search.Filter;

public class FilterManagerImpl implements FilterManager {
	
//	private ZooKeeper zk;
//	private IndexManager indexManager;

	public FilterManagerImpl(IndexManager indexManager) throws IOException {
//		zk = ZooKeeperFactory.getZooKeeper();
//		this.indexManager = indexManager;
	}

	@Override
	public Filter getFilter(String table, String name) {
		return null;
	}

}
