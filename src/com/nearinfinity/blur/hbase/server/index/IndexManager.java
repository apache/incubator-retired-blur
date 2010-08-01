package com.nearinfinity.blur.hbase.server.index;

import java.util.Map;

import org.apache.lucene.index.IndexReader;


public interface IndexManager {
	
	Map<String,IndexReader> getCurrentIndexReaders();

}
