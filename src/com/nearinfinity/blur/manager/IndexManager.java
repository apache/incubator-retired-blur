package com.nearinfinity.blur.manager;

import java.util.Map;

import org.apache.lucene.index.IndexReader;


public interface IndexManager {
	
	Map<String,IndexReader> getCurrentIndexReaders();

}
