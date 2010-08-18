package com.nearinfinity.blur.manager;

import java.util.Map;

import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;


public interface IndexManager extends UpdatableManager {
	
	Directory getDirectory(SuperIndexReader indexReader);
	Map<String, SuperIndexReader> getIndexReaders(String table);

}
