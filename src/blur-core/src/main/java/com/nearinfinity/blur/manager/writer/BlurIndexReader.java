package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public class BlurIndexReader extends BlurIndex {
    
    private IndexReader reader;
    
    public BlurIndexReader(IndexReader reader) {
        this.reader = reader;
    }

    @Override
    public void close() {

    }

    @Override
    public IndexReader getIndexReader(boolean forceRefresh) {
        reader.incRef();
        return reader;
    }

    @Override
    public boolean replaceRow(Row row) {
        return false;
    }

    @Override
    public void refresh() throws IOException {
        
    }
}
