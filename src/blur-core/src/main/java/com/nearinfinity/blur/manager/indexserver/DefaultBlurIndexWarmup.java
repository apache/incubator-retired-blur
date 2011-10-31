package com.nearinfinity.blur.manager.indexserver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;


public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  @Override
  public void warmBlurIndex(String table, String shard, IndexReader reader, AtomicBoolean isClosed) {

  }

}
