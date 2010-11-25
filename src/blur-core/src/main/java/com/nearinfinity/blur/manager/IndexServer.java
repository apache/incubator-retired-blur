package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Selector;

public interface IndexServer {

    Map<String, IndexReader> getIndexReaders(String table) throws IOException;

    IndexReader getIndexReader(String table, Selector selector) throws IOException, MissingShardException;
    
    Similarity getSimilarity();

    Analyzer getAnalyzer(String table);
    
    void close();

}
