package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

public interface IndexServer {
    
    public enum TABLE_STATUS {
        ENABLED,
        DISABLED
    }
    
    List<String> getControllerServerList();
    
    List<String> getShardServerList();
    
    Similarity getSimilarity(String table);
    
    TABLE_STATUS getTableStatus(String table);
    
    Analyzer getAnalyzer(String table);

    Map<String, IndexReader> getIndexReaders(String table) throws IOException;

    List<String> getTableList();
    
    void close();

}
