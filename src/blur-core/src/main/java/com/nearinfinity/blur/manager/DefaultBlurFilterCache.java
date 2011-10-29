package com.nearinfinity.blur.manager;

import org.apache.lucene.search.Filter;

import com.nearinfinity.blur.manager.writer.BlurIndex;

public class DefaultBlurFilterCache extends BlurFilterCache {

  @Override
  public Filter storePreFilter(String table, String filterStr, Filter filter) {
    return filter;
  }

  @Override
  public Filter storePostFilter(String table, String filterStr, Filter filter) {
    return filter;
  }

  @Override
  public Filter fetchPreFilter(String table, String filterStr) {
    return null;
  }

  @Override
  public Filter fetchPostFilter(String table, String filterStr) {
    return null;
  }

  @Override
  public void closing(String table, String shard, BlurIndex index) {
    
  }

  @Override
  public void opening(String table, String shard, BlurIndex index) {
    
  }
}