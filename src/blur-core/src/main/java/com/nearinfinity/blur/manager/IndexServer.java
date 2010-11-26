package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

public interface IndexServer {
    
    Similarity getSimilarity();

    Analyzer getAnalyzer(String table);

    Map<String, IndexReader> getIndexReaders(String table) throws IOException;

    void close();

}
