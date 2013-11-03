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
package org.apache.blur.filter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;

public class FilterCache extends Filter {

  private static final Log LOG = LogFactory.getLog(FilterCache.class);

  private static final DocIdSet EMPTY_DOCIDSET = new DocIdSet() {
    @Override
    public DocIdSetIterator iterator() throws IOException {
      return DocIdSetIterator.empty();
    }
  };

  private final Map<Object, DocIdSet> _cache = Collections.synchronizedMap(new WeakHashMap<Object, DocIdSet>());
  private final Map<Object, Object> _lockMap = Collections.synchronizedMap(new WeakHashMap<Object, Object>());
  private final Filter _filter;
  private final String _id;
  private final AtomicLong _hits = new AtomicLong();
  private final AtomicLong _misses = new AtomicLong();

  public FilterCache(String id, Filter filter) {
    _id = id;
    _filter = filter;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    AtomicReader reader = context.reader();
    Object key = reader.getCoreCacheKey();
    DocIdSet docIdSet = _cache.get(key);
    if (docIdSet != null) {
      _hits.incrementAndGet();
      return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
    }
    // This will only allow a single instance be created per reader per filter
    Object lock = getLock(key);
    synchronized (lock) {
      SegmentReader segmentReader = getSegmentReader(reader);
      if (segmentReader == null) {
        LOG.warn("Could not find SegmentReader from [{0}]", reader);
        return _filter.getDocIdSet(context, acceptDocs);
      }
      Directory directory = getDirectory(segmentReader);
      if (directory == null) {
        LOG.warn("Could not find Directory from [{0}]", segmentReader);
        return _filter.getDocIdSet(context, acceptDocs);
      }
      _misses.incrementAndGet();
      String segmentName = segmentReader.getSegmentName();
      docIdSet = docIdSetToCache(_filter.getDocIdSet(context, null), reader, segmentName, directory);
      _cache.put(key, docIdSet);
      return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
    }
  }

  private synchronized Object getLock(Object key) {
    Object lock = _lockMap.get(key);
    if (lock == null) {
      lock = new Object();
      _lockMap.put(key, lock);
    }
    return lock;
  }

  private DocIdSet docIdSetToCache(DocIdSet docIdSet, AtomicReader reader, String segmentName, Directory directory)
      throws IOException {
    if (docIdSet == null) {
      // this is better than returning null, as the nonnull result can be cached
      return EMPTY_DOCIDSET;
    } else if (docIdSet.isCacheable()) {
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      // null is allowed to be returned by iterator(),
      // in this case we wrap with the empty set,
      // which is cacheable.
      if (it == null) {
        return EMPTY_DOCIDSET;
      } else {
        final IndexFileBitSet bits = new IndexFileBitSet(reader.maxDoc(), _id, segmentName, directory);
        if (!bits.exists()) {
          bits.create(it);
        }
        bits.load();
        return bits;
      }
    }
  }

  private Directory getDirectory(SegmentReader reader) {
    return reader.directory();
  }

  private SegmentReader getSegmentReader(AtomicReader reader) {
    if (reader instanceof SegmentReader) {
      return (SegmentReader) reader;
    }
    return null;
  }

  public long getHits() {
    return _hits.get();
  }

  public long getMisses() {
    return _misses.get();
  }

  @Override
  public String toString() {
    return "FilterCache(" + _id + "," + _filter + ")";
  }
}
