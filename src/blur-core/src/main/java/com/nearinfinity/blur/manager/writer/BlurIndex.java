package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.thrift.generated.Row;

public abstract class BlurIndex {

  public abstract boolean replaceRow(boolean wal, Row row) throws IOException;

  public abstract IndexReader getIndexReader(boolean forceRefresh) throws IOException;

  public abstract void close() throws IOException;

  public abstract void refresh() throws IOException;

  public abstract void deleteRow(boolean wal, String rowId) throws IOException;

}
