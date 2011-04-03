package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurIndex {
    
    public abstract boolean replaceRow(Collection<Row> rows) throws IOException;
    
    public abstract IndexReader getIndexReader() throws IOException;

    public abstract void close() throws IOException;

}
