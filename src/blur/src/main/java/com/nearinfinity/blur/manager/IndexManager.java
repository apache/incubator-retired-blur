package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;


public interface IndexManager {
	
	Map<String, IndexReader> getIndexReaders(String table) throws IOException;

}
