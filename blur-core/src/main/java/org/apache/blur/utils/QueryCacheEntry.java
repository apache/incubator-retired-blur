package org.apache.blur.utils;

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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;


public class QueryCacheEntry {
  public BlurResults results;
  public long timestamp;
  public SortedSet<String> shards;

  public QueryCacheEntry(BlurResults cacheResults) {
    results = cacheResults;
    timestamp = System.currentTimeMillis();
    if (cacheResults != null && cacheResults.shardInfo != null) {
      shards = new TreeSet<String>(cacheResults.shardInfo.keySet());
    } else {
      shards = new TreeSet<String>();
    }
  }

  public BlurResults getBlurResults(BlurQuery blurQuery) {
    BlurResults blurResults = new BlurResults(results);
    blurResults.query = blurQuery;
    return blurResults;
  }
}
