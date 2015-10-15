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
package org.apache.blur.server;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.blur.lucene.search.SuperQuery;
import org.apache.blur.lucene.security.index.AccessControlFactory;
import org.apache.blur.lucene.security.index.FilterAccessControlFactory;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class BlurSecureIndexSearcherTest {

  @Test
  public void testQueryFilterWrap1() throws IOException {
    IndexReader r = getIndexReader();
    AccessControlFactory accessControlFactory = new FilterAccessControlFactory();
    Collection<String> readAuthorizations = new ArrayList<String>();
    Collection<String> discoverAuthorizations = new ArrayList<String>();
    Set<String> discoverableFields = new HashSet<String>(Arrays.asList("rowid"));
    BlurSecureIndexSearcher blurSecureIndexSearcher = new BlurSecureIndexSearcher(r, null, accessControlFactory,
        readAuthorizations, discoverAuthorizations, discoverableFields, null);
    Query wrapFilter;
    Query query = new TermQuery(new Term("a", "b"));
    Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
    {
      Term primeDocTerm = new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE);
      ScoreType scoreType = ScoreType.SUPER;
      SuperQuery superQuery = new SuperQuery(query, scoreType, primeDocTerm);
      wrapFilter = blurSecureIndexSearcher.wrapFilter(superQuery, filter);
      System.out.println(wrapFilter);
    }
    {
      assertTrue(wrapFilter instanceof SuperQuery);
      SuperQuery sq = (SuperQuery) wrapFilter;
      Query inner = sq.getQuery();
      assertTrue(inner instanceof FilteredQuery);
      FilteredQuery filteredQuery = (FilteredQuery) inner;
      Query innerFilteredQuery = filteredQuery.getQuery();
      assertEquals(innerFilteredQuery, query);
      assertTrue(filteredQuery.getFilter() == filter);
    }
  }

  @Test
  public void testQueryFilterWrap2() throws IOException {
    IndexReader r = getIndexReader();
    AccessControlFactory accessControlFactory = new FilterAccessControlFactory();
    Collection<String> readAuthorizations = new ArrayList<String>();
    Collection<String> discoverAuthorizations = new ArrayList<String>();
    Set<String> discoverableFields = new HashSet<String>(Arrays.asList("rowid"));
    BlurSecureIndexSearcher blurSecureIndexSearcher = new BlurSecureIndexSearcher(r, null, accessControlFactory,
        readAuthorizations, discoverAuthorizations, discoverableFields, null);
    Query wrapFilter;
    Query query = new TermQuery(new Term("a", "b"));
    Filter filter = new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
    {
      Term primeDocTerm = new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE);
      ScoreType scoreType = ScoreType.SUPER;
      SuperQuery superQuery = new SuperQuery(query, scoreType, primeDocTerm);
      BooleanQuery booleanQuery = new BooleanQuery();
      booleanQuery.add(superQuery, Occur.MUST);
      wrapFilter = blurSecureIndexSearcher.wrapFilter(booleanQuery, filter);
      System.out.println(wrapFilter);
    }
    {
      assertTrue(wrapFilter instanceof BooleanQuery);
      BooleanQuery booleanQuery = (BooleanQuery) wrapFilter;
      assertEquals(1, booleanQuery.clauses().size());
      BooleanClause booleanClause = booleanQuery.clauses().get(0);
      Query innerClause = booleanClause.getQuery();

      assertTrue(innerClause instanceof SuperQuery);
      SuperQuery sq = (SuperQuery) innerClause;
      Query inner = sq.getQuery();
      assertTrue(inner instanceof FilteredQuery);
      FilteredQuery filteredQuery = (FilteredQuery) inner;
      Query innerFilteredQuery = filteredQuery.getQuery();
      assertEquals(innerFilteredQuery, query);
      assertTrue(filteredQuery.getFilter() == filter);
    }
  }

  private IndexReader getIndexReader() throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.close();
    return DirectoryReader.open(dir);
  }

}
