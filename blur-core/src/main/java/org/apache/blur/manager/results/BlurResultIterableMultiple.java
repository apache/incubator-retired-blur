package org.apache.blur.manager.results;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.io.IOUtils;

public class BlurResultIterableMultiple implements BlurResultIterable {

  private static final Log LOG = LogFactory.getLog(BlurResultIterableMultiple.class);

  private long totalResults;
  private Map<String, Long> shardInfo = new TreeMap<String, Long>();
  private long skipTo;
  private List<BlurResultIterable> results = new ArrayList<BlurResultIterable>();

  public void addBlurResultIterable(BlurResultIterable iterable) {
    totalResults += iterable.getTotalResults();
    shardInfo.putAll(iterable.getShardInfo());
    results.add(iterable);
  }

  @Override
  public Map<String, Long> getShardInfo() {
    return shardInfo;
  }

  @Override
  public long getTotalResults() {
    return totalResults;
  }

  @Override
  public void skipTo(long skipTo) {
    this.skipTo = skipTo;
  }

  @Override
  public BlurIterator<BlurResult, BlurException> iterator() throws BlurException {
    MultipleHitsIterator iterator = new MultipleHitsIterator(results);
    long start = 0;
    Tracer trace = Trace.trace("blurResultsIterable - iterator - skipping", Trace.param("skipTo", skipTo));
    while (iterator.hasNext() && start < skipTo) {
      iterator.next();
      start++;
    }
    trace.done();
    return iterator;
  }

  public static class MultipleHitsIterator implements BlurIterator<BlurResult, BlurException> {

    private final List<PeekableIterator<BlurResult, BlurException>> _iterators = new ArrayList<PeekableIterator<BlurResult, BlurException>>();
    private final int _length;
    private long _position = 0;

    public MultipleHitsIterator(List<BlurResultIterable> hits) throws BlurException {
      for (BlurResultIterable hitsIterable : hits) {
        BlurIterator<BlurResult, BlurException> iterator = hitsIterable.iterator();
        PeekableIterator<BlurResult, BlurException> peekableIterator = PeekableIterator.wrap(iterator);
        _iterators.add(peekableIterator);
      }
      _length = _iterators.size();
    }

    @Override
    public boolean hasNext() throws BlurException {
      for (int i = 0; i < _length; i++) {
        if (_iterators.get(i).hasNext()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public BlurResult next() throws BlurException {
      Collections.sort(_iterators, BlurUtil.HITS_PEEKABLE_ITERATOR_COMPARATOR);
      _position++;
      return _iterators.get(0).next();
    }

    @Override
    public long getPosition() throws BlurException {
      return _position;
    }
  }

  @Override
  public void close() throws IOException {
    for (BlurResultIterable it : results) {
      IOUtils.cleanup(LOG, it);
    }
  }
}
