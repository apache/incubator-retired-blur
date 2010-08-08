package com.nearinfinity.blur.manager;

import org.apache.lucene.analysis.Analyzer;

public interface TableAnalyzerManager {
	
	Analyzer getAnalyzer(String table);

}
