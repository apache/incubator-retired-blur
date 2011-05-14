package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurIndex {
    
    public abstract boolean replaceRow(Iterable<Row> rows) throws IOException;
    
    public abstract IndexReader getIndexReader() throws IOException;

    public abstract void close() throws IOException;

}
