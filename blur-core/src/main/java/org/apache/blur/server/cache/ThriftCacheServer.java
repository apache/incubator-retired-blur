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
package org.apache.blur.server.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.server.FilteredBlurServer;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.ShardUtil;

public class ThriftCacheServer extends FilteredBlurServer {

  private final ThriftCache _thriftCache;
  private final IndexServer _indexServer;

  public ThriftCacheServer(BlurConfiguration configuration, Iface iface, IndexServer indexServer,
      ThriftCache thriftCache) {
    super(configuration, iface, true);
    _thriftCache = thriftCache;
    _indexServer = indexServer;
  }

  @Override
  public TableStats tableStats(String table) throws BlurException, TException {
    ThriftCacheKey<TableStats> key = _thriftCache.getKey(table, getShards(table), null, TableStats.class);
    TableStats results = _thriftCache.get(key, TableStats.class);
    if (results != null) {
      return results;
    }
    return _thriftCache.put(key, super.tableStats(table));
  }

  private int[] getShards(String table) throws BlurException {
    try {
      Set<String> keySet = _indexServer.getIndexes(table).keySet();
      int[] shards = new int[keySet.size()];
      int i = 0;
      for (String s : keySet) {
        int shardIndex = ShardUtil.getShardIndex(s);
        shards[i++] = shardIndex;
      }
      Arrays.sort(shards);
      return shards;
    } catch (IOException e) {
      throw new BException("Unknown error while trying to get current shards for table [{0}]", e, table);
    }
  }

  @Override
  public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
    boolean useCacheIfPresent = blurQuery.isUseCacheIfPresent();
    boolean cacheResult = blurQuery.isCacheResult();
    BlurQuery copy = new BlurQuery(blurQuery);
    // These are the fields that play no part in the actual results returned.
    // So we are going to normalize the values so that we can reuse the cache
    // results.
    copy.useCacheIfPresent = false;
    copy.maxQueryTime = 0;
    copy.uuid = null;
    copy.userContext = null;
    copy.cacheResult = false;
    copy.startTime = 0;
    ThriftCacheKey<BlurQuery> key = _thriftCache.getKey(table, getShards(table), copy, BlurQuery.class);
    if (useCacheIfPresent) {
      BlurResults results = _thriftCache.get(key, BlurResults.class);
      if (results != null) {
        return results;
      }
    }
    BlurResults blurResults = super.query(table, blurQuery);
    if (cacheResult) {
      return _thriftCache.put(key, blurResults);
    }
    return blurResults;
  }

  @Override
  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    Selector copy = new Selector(selector);
    ThriftCacheKey<Selector> key = _thriftCache.getKey(table, getShards(table), copy, Selector.class);
    FetchResult results = _thriftCache.get(key, FetchResult.class);
    if (results != null) {
      return results;
    }
    return _thriftCache.put(key, super.fetchRow(table, selector));
  }

  @Override
  public List<FetchResult> fetchRowBatch(String table, List<Selector> selectors) throws BlurException, TException {
    Map<Integer, FetchResult> resultMap = new TreeMap<Integer, FetchResult>();

    // Maps cache miss request list index to original request list.
    Map<Integer, Integer> requestMapping = new HashMap<Integer, Integer>();
    List<Selector> selectorRequest = new ArrayList<Selector>();

    // Gather hits that are already in cache.
    for (int i = 0; i < selectors.size(); i++) {
      Selector selector = selectors.get(i);
      Selector copy = new Selector(selector);
      ThriftCacheKey<Selector> key = _thriftCache.getKey(table, getShards(table), copy, Selector.class);
      FetchResult fetchResult = _thriftCache.get(key, FetchResult.class);
      if (fetchResult != null) {
        resultMap.put(i, fetchResult);
      } else {
        int index = selectorRequest.size();
        requestMapping.put(index, i);
        selectorRequest.add(selector);
      }
    }

    if (selectorRequest.size() != 0) {
      List<FetchResult> missingResults = super.fetchRowBatch(table, selectorRequest);
      for (int i = 0; i < missingResults.size(); i++) {
        Selector selector = selectorRequest.get(i);
        FetchResult fetchResult = missingResults.get(i);
        ThriftCacheKey<Selector> key = _thriftCache.getKey(table, getShards(table), new Selector(selector),
            Selector.class);
        _thriftCache.put(key, fetchResult);
        int originalIndex = requestMapping.get(i);
        resultMap.put(originalIndex, fetchResult);
      }
    }
    return toList(resultMap);
  }

  private List<FetchResult> toList(Map<Integer, FetchResult> resultMap) {
    return new ArrayList<FetchResult>(resultMap.values());
  }

}
