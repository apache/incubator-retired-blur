/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.results;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurResultIterableSimple implements BlurResultIterable {

  private List<BlurResult> results;
  private Map<String, Long> shardInfo;
  private long skipTo;

  public BlurResultIterableSimple(String shard, List<BlurResult> hits) {
    Collections.sort(hits, BlurConstants.HITS_COMPARATOR);
    this.results = hits;
    this.shardInfo = new TreeMap<String, Long>();
    this.shardInfo.put(shard, (long) hits.size());
  }

  @Override
  public Map<String, Long> getShardInfo() {
    return shardInfo;
  }

  @Override
  public long getTotalResults() {
    return results.size();
  }

  @Override
  public void skipTo(long skipTo) {
    this.skipTo = skipTo;
  }

  @Override
  public Iterator<BlurResult> iterator() {
    long start = 0;
    Iterator<BlurResult> iterator = results.iterator();
    while (iterator.hasNext() && start < skipTo) {
      iterator.next();
      start++;
    }
    return iterator;
  }

}
