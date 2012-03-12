package com.nearinfinity.blur.manager.writer;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Row;

public class BlurIndexReader extends AbstractBlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexReader.class);

  public void init() throws IOException {
    initIndexWriterConfig();
    initIndexReader(IndexReader.open(getDirectory(), true));
  }

  @Override
  public synchronized void refresh() throws IOException {
    // Override so that we can call within synchronized method
    super.refresh();
  }

  @Override
  public void close() throws IOException {
    super.close();
    LOG.info("Reader for table [{0}] shard [{1}] closed.",getTable(),getShard());
  }

  @Override
  public void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException {
    throw new RuntimeException("Read-only shard");
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    // Do nothing
  }
}
