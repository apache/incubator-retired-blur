package com.nearinfinity.blur.manager.writer;

import java.util.Collection;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurIndex {
    
    public abstract boolean replaceRow(Collection<Row> rows);
    
    public abstract IndexReader getIndexReader();

    public abstract void close();

}
