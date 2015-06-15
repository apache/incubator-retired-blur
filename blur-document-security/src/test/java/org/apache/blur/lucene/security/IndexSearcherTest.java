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
package org.apache.blur.lucene.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.blur.lucene.security.DocumentAuthorizations;
import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.blur.lucene.security.DocumentVisibilityEvaluator;
import org.apache.blur.lucene.security.index.AccessControlFactory;
import org.apache.blur.lucene.security.index.AccessControlWriter;
import org.apache.blur.lucene.security.index.FilterAccessControlFactory;
import org.apache.blur.lucene.security.index.SecureAtomicReader;
import org.apache.blur.lucene.security.search.SecureIndexSearcher;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class IndexSearcherTest {

  private static final Collection<String> EMPTY = Arrays.asList(new String[] {});
  private AccessControlFactory _accessControlFactory = new FilterAccessControlFactory();

  @Test
  public void test1() throws ParseException, IOException {
    runTest(1, list("d", "a", "b"));
  }

  @Test
  public void test2() throws ParseException, IOException {
    runTest(2, list("c", "a", "b"));
  }

  @Test
  public void test3() throws ParseException, IOException {
    runTest(0, list("x"));
  }

  @Test
  public void test4() throws ParseException, IOException {
    runTest(3, list("c", "a", "b"), list("c", "a", "b"), list("_read_", "_discover_"));
  }

  @Test
  public void test5() throws ParseException, IOException {
    runTest(3, list("c", "a", "b"), list("c", "a", "b"), list("_read_", "_discover_"));
  }

  private void runTest(int expected, Collection<String> readAuthorizations) throws IOException, ParseException {
    runTest(expected, readAuthorizations, EMPTY, EMPTY);
  }

  private void runTest(int expected, Collection<String> readAuthorizations, Collection<String> discoverAuthorizations,
      Collection<String> discoverableFields) throws IOException, ParseException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new StandardAnalyzer(Version.LUCENE_43));
    Directory dir = new RAMDirectory();
    {
      IndexWriter writer = new IndexWriter(dir, conf);
      writer.addDocument(getEmpty());
      writer.commit();
      writer.addDocument(getDoc(0, "(a&b)|d", null, "f1", "f2"));
      writer.addDocument(getDoc(1, "a&b&c", null, "f1", "f2"));
      writer.addDocument(getDoc(2, "a&b&c&e", "a&b&c", "f1", "f2"));
      writer.addDocument(getDoc(3, null, null, "f1", "f2"));// can't find
      writer.close(false);
    }
    DirectoryReader reader = DirectoryReader.open(dir);
    validate(expected, 2, readAuthorizations, discoverAuthorizations, discoverableFields, dir, reader);
    {
      IndexWriter writer = new IndexWriter(dir, conf);
      writer.deleteDocuments(new Term("id", "0"));
      writer.addDocument(getDoc(0, "(a&b)|d", null, "f1", "f2"));
      writer.close(false);
    }
    reader = DirectoryReader.openIfChanged(reader);
    validate(expected, 3, readAuthorizations, discoverAuthorizations, discoverableFields, dir, reader);
    {
      IndexWriter writer = new IndexWriter(dir, conf);
      writer.deleteDocuments(new Term("id", "1"));
      writer.addDocument(getDoc(1, "a&b&c", null, "f1", "f2"));
      writer.close(false);
    }
    reader = DirectoryReader.openIfChanged(reader);
    validate(expected, 4, readAuthorizations, discoverAuthorizations, discoverableFields, dir, reader);
  }

  private void validate(int expected, int leafCount, Collection<String> readAuthorizations,
      Collection<String> discoverAuthorizations, Collection<String> discoverableFields, Directory dir,
      IndexReader reader) throws IOException {
    List<AtomicReaderContext> leaves = reader.leaves();
    assertEquals(leafCount, leaves.size());
    SecureIndexSearcher searcher = new SecureIndexSearcher(reader, getAccessControlFactory(), readAuthorizations,
        discoverAuthorizations, toSet(discoverableFields));
    TopDocs topDocs;
    Query query = new MatchAllDocsQuery();
    {
      topDocs = searcher.search(query, 10);
      assertEquals(expected, topDocs.totalHits);
    }
    DocumentAuthorizations readDocumentAuthorizations = new DocumentAuthorizations(readAuthorizations);
    DocumentAuthorizations discoverDocumentAuthorizations = new DocumentAuthorizations(discoverAuthorizations);
    DocumentVisibilityEvaluator readVisibilityEvaluator = new DocumentVisibilityEvaluator(readDocumentAuthorizations);
    DocumentVisibilityEvaluator discoverVisibilityEvaluator = new DocumentVisibilityEvaluator(
        discoverDocumentAuthorizations);
    for (int i = 0; i < topDocs.totalHits & i < topDocs.scoreDocs.length; i++) {
      Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
      String read = doc.get("_read_");
      String discover = doc.get("_discover_");
      if (read != null && discover != null) {
        DocumentVisibility readVisibility = new DocumentVisibility(read);
        DocumentVisibility discoverVisibility = new DocumentVisibility(discover);
        assertTrue(readVisibilityEvaluator.evaluate(readVisibility)
            || discoverVisibilityEvaluator.evaluate(discoverVisibility));
      } else if (read != null) {
        DocumentVisibility readVisibility = new DocumentVisibility(read);
        assertTrue(readVisibilityEvaluator.evaluate(readVisibility));
      } else if (discover != null) {
        DocumentVisibility discoverVisibility = new DocumentVisibility(discover);
        assertTrue(discoverVisibilityEvaluator.evaluate(discoverVisibility));
        // Since this document is only discoverable validate fields that are
        // being returned.
        validateDiscoverFields(doc, discoverableFields);
      } else {
        fail("Should not fetch empty document.");
      }
    }
    searcher.search(query, new Collector() {

      @Override
      public void setScorer(Scorer scorer) throws IOException {
      }

      @Override
      public void setNextReader(AtomicReaderContext context) throws IOException {
        assertTrue(context.reader() instanceof SecureAtomicReader);
      }

      @Override
      public void collect(int doc) throws IOException {

      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return false;
      }
    });
  }

  private Iterable<? extends IndexableField> getEmpty() {
    return new Document();
  }

  private void validateDiscoverFields(Document doc, Collection<String> discoverableFields) {
    Set<String> fields = new HashSet<String>(discoverableFields);
    for (IndexableField indexableField : doc.getFields()) {
      assertTrue(fields.contains(indexableField.name()));
    }
  }

  private Set<String> toSet(Collection<String> col) {
    if (col == null) {
      return null;
    }
    return new HashSet<String>(col);
  }

  private AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  private Iterable<? extends IndexableField> getDoc(int docId, String read, String discover, String field1,
      String field2) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(docId), Store.YES));
    AccessControlWriter writer = _accessControlFactory.getWriter();
    doc.add(new StringField("f1", field1, Store.YES));
    doc.add(new StringField("f2", field2, Store.YES));
    doc.add(new TextField("text", "constant text", Store.YES));
    Iterable<IndexableField> fields = doc;
    if (read != null) {
      fields = writer.addReadVisiblity(read, doc);
    }
    if (discover != null) {
      fields = writer.addDiscoverVisiblity(discover, fields);
    }
    return fields;
  }

  private List<String> list(String... strs) {
    return Arrays.asList(strs);
  }
}