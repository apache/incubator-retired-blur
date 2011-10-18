package com.nearinfinity.blur.manager;

import org.apache.lucene.search.Filter;

public abstract class BlurFilterCache {

  public abstract Filter fetchPreFilter(String table, String filterStr);

  public abstract Filter fetchPostFilter(String table, String filterStr);

  public abstract Filter storePreFilter(String table, String filterStr, Filter filter);

  public abstract Filter storePostFilter(String table, String filterStr, Filter filter);

}
