package org.apache.blur.thrift;
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
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;

import com.google.common.collect.Lists;

public class QueryBuilder {
  private String queryString;
  private Selector selector = new Selector();
  private List<Facet> facets = Lists.newArrayList();
 
  
  private QueryBuilder(String queryString) {
    this.queryString = queryString;
  }
  
  public static QueryBuilder define(String q) {
    return new QueryBuilder(q);
  }
  
  public QueryBuilder withFacet(String facetQuery, long facetCount) {
    facets.add(new Facet(facetQuery, facetCount));
    return this;
  }
  
  public BlurQuery build() {
    BlurQuery blurQuery = new BlurQuery();
    Query queryRow = new Query();
    queryRow.setQuery(queryString);
    blurQuery.setQuery(queryRow);
    blurQuery.setUseCacheIfPresent(false);
    blurQuery.setCacheResult(false);
    blurQuery.setSelector(selector);
    blurQuery.setFacets(facets);
    
    return blurQuery;
  }

  public BlurResults run(String table, Iface client) throws BlurException, TException {
    return client.query(table, build());
  }
}
