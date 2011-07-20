package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Transaction;

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
    public boolean replaceRow(Transaction transaction, Row row) {
        return false;
    }

    @Override
    public void commit(Transaction transaction) throws IOException {
        
    }

    @Override
    public void abort(Transaction transaction) {
        
    }

}
