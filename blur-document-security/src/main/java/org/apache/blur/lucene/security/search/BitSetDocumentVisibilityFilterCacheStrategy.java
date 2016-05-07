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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

import org.apache.blur.lucene.security.index.SecureAtomicReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
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
    int maxDoc = reader.maxDoc();
    final Key key = new Key(fieldName, term, reader.getCoreCacheKey());
    LOG.debug("Creating new bitset for key [" + key + "] on index [" + reader + "]");
    return new Builder() {

      private OpenBitSet bitSet = new OpenBitSet(maxDoc);

      @Override
      public void or(DocIdSetIterator it) throws IOException {
        LOG.debug("Building bitset for key [" + key + "]");
        int doc;
        while ((doc = it.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
          bitSet.set(doc);
        }
      }

      @Override
      public DocIdSet getDocIdSet() throws IOException {
        SegmentReader segmentReader = getSegmentReader(reader);
        segmentReader.addReaderClosedListener(new ReaderClosedListener() {
          @Override
          public void onClose(IndexReader reader) {
            LOG.debug("Removing old bitset for key [" + key + "]");
            DocIdSet docIdSet = _cache.remove(key);
            if (docIdSet == null) {
              LOG.warn("DocIdSet was missing for key [" + docIdSet + "]");
            }
          }
        });
        long cardinality = bitSet.cardinality();
        DocIdSet cacheDocIdSet;
        if (isFullySet(maxDoc, bitSet, cardinality)) {
          cacheDocIdSet = getFullySetDocIdSet(maxDoc);
        } else if (isFullyEmpty(bitSet, cardinality)) {
          cacheDocIdSet = getFullyEmptyDocIdSet(maxDoc);
        } else {
          cacheDocIdSet = bitSet;
        }
        _cache.put(key, cacheDocIdSet);
        return cacheDocIdSet;
      }
    };
  }

  public static DocIdSet getFullyEmptyDocIdSet(int maxDoc) {
    Bits bits = getFullyEmptyBits(maxDoc);
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return getFullyEmptyDocIdSetIterator(maxDoc);
      }

      @Override
      public Bits bits() throws IOException {
        return bits;
      }

      @Override
      public boolean isCacheable() {
        return true;
      }
    };
  }

  public static DocIdSetIterator getFullyEmptyDocIdSetIterator(int maxDoc) {
    return new DocIdSetIterator() {

      private int _docId = -1;

      @Override
      public int docID() {
        return _docId;
      }

      @Override
      public int nextDoc() throws IOException {
        return _docId = DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) throws IOException {
        return _docId = DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  public static Bits getFullyEmptyBits(int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return false;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }

  public static DocIdSet getFullySetDocIdSet(int maxDoc) {
    Bits bits = getFullySetBits(maxDoc);
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        return getFullySetDocIdSetIterator(maxDoc);
      }

      @Override
      public Bits bits() throws IOException {
        return bits;
      }

      @Override
      public boolean isCacheable() {
        return true;
      }
    };
  }

  public static DocIdSetIterator getFullySetDocIdSetIterator(int maxDoc) {
    return new DocIdSetIterator() {

      private int _docId = -1;

      @Override
      public int advance(int target) throws IOException {
        if (_docId == DocIdSetIterator.NO_MORE_DOCS) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        _docId = target;
        if (_docId >= maxDoc) {
          return _docId = DocIdSetIterator.NO_MORE_DOCS;
        }
        return _docId;
      }

      @Override
      public int nextDoc() throws IOException {
        if (_docId == DocIdSetIterator.NO_MORE_DOCS) {
          return DocIdSetIterator.NO_MORE_DOCS;
        }
        _docId++;
        if (_docId >= maxDoc) {
          return _docId = DocIdSetIterator.NO_MORE_DOCS;
        }
        return _docId;
      }

      @Override
      public int docID() {
        return _docId;
      }

      @Override
      public long cost() {
        return 0l;
      }

    };
  }

  public static Bits getFullySetBits(int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return true;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }

  public static boolean isFullyEmpty(OpenBitSet bitSet, long cardinality) {
    if (cardinality == 0) {
      return true;
    }
    return false;
  }

  public static boolean isFullySet(int maxDoc, OpenBitSet bitSet, long cardinality) {
    if (cardinality >= maxDoc) {
      return true;
    }
    return false;
  }

  public static SegmentReader getSegmentReader(IndexReader indexReader) throws IOException {
    if (indexReader instanceof SegmentReader) {
      return (SegmentReader) indexReader;
    } else if (indexReader instanceof SecureAtomicReader) {
      SecureAtomicReader atomicReader = (SecureAtomicReader) indexReader;
      AtomicReader originalReader = atomicReader.getOriginalReader();
      return getSegmentReader(originalReader);
    } else {
      try {
        Method method = indexReader.getClass().getDeclaredMethod("getOriginalReader", new Class[] {});
        return getSegmentReader((IndexReader) method.invoke(indexReader, new Object[] {}));
      } catch (NoSuchMethodException e) {
        LOG.error("IndexReader cannot find method [getOriginalReader]");
      } catch (SecurityException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      } catch (IllegalArgumentException e) {
        throw new IOException(e);
      } catch (InvocationTargetException e) {
        throw new IOException(e);
      }
    }
    throw new IOException("SegmentReader could not be found [" + indexReader + "].");
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
