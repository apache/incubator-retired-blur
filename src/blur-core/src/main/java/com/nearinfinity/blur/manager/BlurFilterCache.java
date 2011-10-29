package com.nearinfinity.blur.manager;

import org.apache.lucene.search.Filter;

import com.nearinfinity.blur.manager.writer.BlurIndex;

public abstract class BlurFilterCache {

  public abstract Filter fetchPreFilter(String table, String filterStr);

  public abstract Filter fetchPostFilter(String table, String filterStr);

  public abstract Filter storePreFilter(String table, String filterStr, Filter filter);

  public abstract Filter storePostFilter(String table, String filterStr, Filter filter);

  public abstract void closing(String table, String shard, BlurIndex index);

  public abstract void opening(String table, String shard, BlurIndex index);

}
