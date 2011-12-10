package com.nearinfinity.blur.utils;

import com.nearinfinity.blur.thrift.generated.BlurQuery;

public class QueryCacheKey {
  private String table;
  private BlurQuery query;

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public BlurQuery getQuery() {
    return query;
  }

  public void setQuery(BlurQuery query) {
    this.query = query;
  }

  public QueryCacheKey(String table, BlurQuery query) {
    this.table = table;
    this.query = query;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((query == null) ? 0 : query.hashCode());
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    QueryCacheKey other = (QueryCacheKey) obj;
    if (query == null) {
      if (other.query != null)
        return false;
    } else if (!query.equals(other.query))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }

}
