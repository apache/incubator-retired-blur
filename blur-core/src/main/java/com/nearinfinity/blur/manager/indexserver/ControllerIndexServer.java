package com.nearinfinity.blur.manager.indexserver;

import java.util.List;

import org.apache.lucene.index.IndexReader;

public class ControllerIndexServer extends ManagedDistributedIndexServer {

    @Override
    protected void beforeClose(String shard, IndexReader indexReader) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected IndexReader openShard(String table, String shard) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getShardList(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void cleanupLocallyCachedIndexes(String table, String shard) {
        //do nothing        
    }

    @Override
    protected void warmUpIndexes() {
      //do nothing
    }

}
