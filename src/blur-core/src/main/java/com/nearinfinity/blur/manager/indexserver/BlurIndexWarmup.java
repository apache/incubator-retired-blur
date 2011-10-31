package com.nearinfinity.blur.manager.indexserver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;

public abstract class BlurIndexWarmup {

  public abstract void warmBlurIndex(String table, String shard, IndexReader reader, AtomicBoolean isClosed);


}
