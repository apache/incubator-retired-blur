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
package org.apache.blur.lucene.security.search;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;

import com.google.common.collect.MapMaker;

public class BitSetDocumentVisibilityFilterCacheStrategy extends DocumentVisibilityFilterCacheStrategy {

  private static final Log LOG = LogFactory.getLog(BitSetDocumentVisibilityFilterCacheStrategy.class);

  public static final DocumentVisibilityFilterCacheStrategy INSTANCE = new BitSetDocumentVisibilityFilterCacheStrategy();

  private final ConcurrentMap<Key, DocIdSet> _cache;

  public BitSetDocumentVisibilityFilterCacheStrategy() {
    _cache = new MapMaker().makeMap();
  }

  @Override
  public DocIdSet getDocIdSet(String fieldName, BytesRef term, AtomicReader reader) {
    Key key = new Key(fieldName, term, reader.getCoreCacheKey());
    DocIdSet docIdSet = _cache.get(key);
    if (docIdSet != null) {
      LOG.debug("Cache hit for key [" + key + "]");
    } else {
      LOG.debug("Cache miss for key [" + key + "]");
    }
    return docIdSet;
  }

  @Override
  public Builder createBuilder(String fieldName, BytesRef term, final AtomicReader reader) {
    final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
    final Key key = new Key(fieldName, term, reader.getCoreCacheKey());
    LOG.debug("Creating new bitset for key [" + key + "] on index [" + reader + "]");
    return new Builder() {
      @Override
      public void or(DocIdSetIterator it) throws IOException {
        int doc;
        while ((doc = it.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
          bitSet.set(doc);
        }
      }

      @Override
      public DocIdSet getDocIdSet() throws IOException {
        reader.addReaderClosedListener(new ReaderClosedListener() {
          @Override
          public void onClose(IndexReader reader) {
            LOG.debug("Removing old bitset for key [" + key + "]");
            DocIdSet docIdSet = _cache.remove(key);
            if (docIdSet == null) {
              LOG.warn("DocIdSet was missing for key [" + docIdSet + "]");
            }
          }
        });
        _cache.put(key, bitSet);
        return bitSet;
      }
    };
  }

  private static class Key {

    private final Object _object;
    private final BytesRef _term;
    private final String _fieldName;

    public Key(String fieldName, BytesRef term, Object object) {
      _fieldName = fieldName;
      _term = BytesRef.deepCopyOf(term);
      _object = object;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_fieldName == null) ? 0 : _fieldName.hashCode());
      result = prime * result + ((_object == null) ? 0 : _object.hashCode());
      result = prime * result + ((_term == null) ? 0 : _term.hashCode());
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
      Key other = (Key) obj;
      if (_fieldName == null) {
        if (other._fieldName != null)
          return false;
      } else if (!_fieldName.equals(other._fieldName))
        return false;
      if (_object == null) {
        if (other._object != null)
          return false;
      } else if (!_object.equals(other._object))
        return false;
      if (_term == null) {
        if (other._term != null)
          return false;
      } else if (!_term.equals(other._term))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Key [_object=" + _object + ", _fieldName=" + _fieldName + ", _term=" + _term + "]";
    }

  }

  @Override
  public String toString() {
    return "BitSetDocumentVisibilityFilterCacheStrategy [_cache=" + _cache + "]";
  }

}
