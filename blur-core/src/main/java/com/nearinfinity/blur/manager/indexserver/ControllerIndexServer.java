package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

public class ControllerIndexServer extends ManagedDistributedIndexServer {

    private List<String> EMPTY = new ArrayList<String>();

    @Override
    protected void beforeClose(String shard, IndexReader indexReader) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IndexReader openShard(String table, String shard) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Analyzer getAnalyzer(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getShardList(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Similarity getSimilarity(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTableList() {
        return EMPTY;
    }

    @Override
    public TABLE_STATUS getTableStatus(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void cleanupLocallyCachedIndexes(String table, String shard) {
        //do nothing        
    }

}
