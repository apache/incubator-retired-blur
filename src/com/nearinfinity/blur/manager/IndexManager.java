package com.nearinfinity.blur.manager;

import java.util.Map;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;


public interface IndexManager {
	
	Map<String,SuperIndexReader> getCurrentIndexReaders();

}
