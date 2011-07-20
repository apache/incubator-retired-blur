package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Transaction;

public abstract class BlurIndex {
    
    public abstract void abort(Transaction transaction);
    
    public abstract void commit(Transaction transaction) throws IOException;
    
    public abstract boolean replaceRow(Transaction transaction, Row row) throws IOException;
    
    public abstract IndexReader getIndexReader() throws IOException;

    public abstract void close() throws IOException;

}
