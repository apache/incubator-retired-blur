package com.nearinfinity.blur.writer;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurIndex {
    
    public abstract boolean replaceRow(Collection<Row> rows) throws InterruptedException, IOException;
    
    public abstract IndexReader getIndexReader() throws IOException;

}
