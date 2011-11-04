package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;


public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  @Override
  public void warmBlurIndex(String table, String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    releaseReader.release();
  }

}
