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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.blur.lucene.security.index.AccessControlFactory;
import org.apache.blur.lucene.security.index.AccessControlReader;
import org.apache.blur.lucene.security.index.SecureAtomicReader;
import org.apache.blur.lucene.security.index.SecureDirectoryReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class SecureIndexSearcher extends IndexSearcher {

  private final IndexReader _secureIndexReader;
  private final Map<Object, AtomicReaderContext> _leaveMap;
  private final AccessControlFactory _accessControlFactory;
  private final Collection<String> _readAuthorizations;
  private final Collection<String> _discoverAuthorizations;
  private final Set<String> _discoverableFields;
  private final String _defaultReadMaskMessage;
  private AccessControlReader _accessControlReader;

  public SecureIndexSearcher(IndexReader r, AccessControlFactory accessControlFactory,
      Collection<String> readAuthorizations, Collection<String> discoverAuthorizations, Set<String> discoverableFields,
      String defaultReadMaskMessage) throws IOException {
    this(r, null, accessControlFactory, readAuthorizations, discoverAuthorizations, discoverableFields,
        defaultReadMaskMessage);
  }

  public SecureIndexSearcher(IndexReader r, ExecutorService executor, AccessControlFactory accessControlFactory,
      Collection<String> readAuthorizations, Collection<String> discoverAuthorizations, Set<String> discoverableFields,
      String defaultReadMaskMessage) throws IOException {
    this(r.getContext(), executor, accessControlFactory, readAuthorizations, discoverAuthorizations,
        discoverableFields, defaultReadMaskMessage);
  }

  public SecureIndexSearcher(IndexReaderContext context, AccessControlFactory accessControlFactory,
      Collection<String> readAuthorizations, Collection<String> discoverAuthorizations, Set<String> discoverableFields,
      String defaultReadMaskMessage) throws IOException {
    this(context, null, accessControlFactory, readAuthorizations, discoverAuthorizations, discoverableFields,
        defaultReadMaskMessage);
  }

  public SecureIndexSearcher(IndexReaderContext context, ExecutorService executor,
      AccessControlFactory accessControlFactory, Collection<String> readAuthorizations,
      Collection<String> discoverAuthorizations, Set<String> discoverableFields, String defaultReadMaskMessage)
      throws IOException {
    super(context, executor);
    _accessControlFactory = accessControlFactory;
    _readAuthorizations = readAuthorizations;
    _discoverAuthorizations = discoverAuthorizations;
    _discoverableFields = discoverableFields;
    _defaultReadMaskMessage = defaultReadMaskMessage;
    _accessControlReader = _accessControlFactory.getReader(readAuthorizations, discoverAuthorizations,
        discoverableFields, _defaultReadMaskMessage);
    _secureIndexReader = getSecureIndexReader(context);
    List<AtomicReaderContext> leaves = _secureIndexReader.leaves();
    _leaveMap = new HashMap<Object, AtomicReaderContext>();
    for (AtomicReaderContext atomicReaderContext : leaves) {
      AtomicReader atomicReader = atomicReaderContext.reader();
      SecureAtomicReader secureAtomicReader = (SecureAtomicReader) atomicReader;
      AtomicReader originalReader = secureAtomicReader.getOriginalReader();
      Object coreCacheKey = originalReader.getCoreCacheKey();
      _leaveMap.put(coreCacheKey, atomicReaderContext);
    }
  }

  protected AtomicReader getSecureAtomicReader(AtomicReader atomicReader) throws IOException {
    return SecureAtomicReader.create(_accessControlFactory, atomicReader, _readAuthorizations, _discoverAuthorizations,
        _discoverableFields, _defaultReadMaskMessage);
  }

  protected IndexReader getSecureIndexReader(IndexReaderContext context) throws IOException {
    IndexReader indexReader = context.reader();
    if (indexReader instanceof DirectoryReader) {
      return SecureDirectoryReader.create(_accessControlFactory, (DirectoryReader) indexReader, _readAuthorizations,
          _discoverAuthorizations, _discoverableFields, _defaultReadMaskMessage);
    } else if (indexReader instanceof AtomicReader) {
      return SecureAtomicReader.create(_accessControlFactory, (AtomicReader) indexReader, _readAuthorizations,
          _discoverAuthorizations, _discoverableFields, _defaultReadMaskMessage);
    }
    throw new IOException("IndexReader type [" + indexReader.getClass() + "] not supported.");
  }

  @Override
  public IndexReader getIndexReader() {
    return _secureIndexReader;
  }

  protected Filter getSecureFilter() throws IOException {
    return _accessControlReader.getQueryFilter();
  }

  protected Collector getSecureCollector(final Collector collector) {
    return new Collector() {

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        collector.setScorer(scorer);
      }

      @Override
      public void setNextReader(AtomicReaderContext context) throws IOException {
        Object key = context.reader().getCoreCacheKey();
        AtomicReaderContext atomicReaderContext = _leaveMap.get(key);
        collector.setNextReader(atomicReaderContext);
      }

      @Override
      public void collect(int doc) throws IOException {
        collector.collect(doc);
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return collector.acceptsDocsOutOfOrder();
      }
    };
  }

  @Override
  public Weight createNormalizedWeight(Query query) throws IOException {
    return super.createNormalizedWeight(wrapFilter(query, getSecureFilter()));
  }

  @Override
  protected void search(List<AtomicReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    super.search(leaves, weight, getSecureCollector(collector));
  }

  public Document doc(int docID) throws IOException {
    return _secureIndexReader.document(docID);
  }

  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    _secureIndexReader.document(docID, fieldVisitor);
  }

  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return _secureIndexReader.document(docID, fieldsToLoad);
  }

}
