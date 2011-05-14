package com.nearinfinity.blur.manager.writer;

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
    public IndexReader getIndexReader() {
        reader.incRef();
        return reader;
    }

    @Override
    public boolean replaceRow(Iterable<Row> rows) {
        return false;
    }

}
