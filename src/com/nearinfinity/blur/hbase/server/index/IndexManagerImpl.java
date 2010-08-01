package com.nearinfinity.blur.hbase.server.index;

import java.util.Map;

import org.apache.lucene.index.IndexReader;

public class IndexManagerImpl implements IndexManager {

	public IndexManagerImpl(DirectoryManager directoryManager) {

	}

	@Override
	public Map<String, IndexReader> getCurrentIndexReaders() {
		return null;
	}

}
