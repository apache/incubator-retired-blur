package com.nearinfinity.blur.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
