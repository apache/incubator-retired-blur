package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BitVector;

/**
 * Represented as a coupled graph of class instances, this all-in-memory index
 * store implementation delivers search results up to a 100 times faster than
 * the file-centric RAMDirectory at the cost of greater RAM consumption.
 * <p>
 * 
 * @lucene.experimental <p>
 *                      There are no read and write locks in this store.
 *                      {@link InstantiatedIndexReader}
 *                      {@link InstantiatedIndexReader#isCurrent()} all the time
 *                      and
 *                      {@link org.apache.lucene.store.instantiated.InstantiatedIndexWriter}
 *                      will attempt to update instances of the object graph in
 *                      memory at the same time as a searcher is reading from
 *                      it.
 * 
 *                      Consider using InstantiatedIndex as if it was immutable.
 */
public class InstantiatedIndex implements Serializable, Closeable {

  private static final long serialVersionUID = 1l;

  private long version = System.currentTimeMillis();

  private InstantiatedDocument[] documentsByNumber;
  private BitVector deletedDocuments;
  private Map<String, Map<String, InstantiatedTerm>> termsByFieldAndText;
  private InstantiatedTerm[] orderedTerms;
  private Map<String, byte[]> normsByFieldNameAndDocumentNumber;

  private FieldSettings fieldSettings;

  /**
   * Creates an empty instantiated index for you to fill with data using an
   * {@link org.apache.lucene.store.instantiated.InstantiatedIndexWriter}.
   */
  public InstantiatedIndex() {
    initialize();
  }

  void initialize() {
    // todo: clear index without loosing memory (uncouple stuff)
    termsByFieldAndText = new HashMap<String, Map<String, InstantiatedTerm>>();
    orderedTerms = new InstantiatedTerm[0];
    documentsByNumber = new InstantiatedDocument[0];
    normsByFieldNameAndDocumentNumber = new HashMap<String, byte[]>();
    fieldSettings = new FieldSettings();
  }

  public InstantiatedIndexWriter indexWriterFactory(Analyzer analyzer, boolean create) throws IOException {
    return new InstantiatedIndexWriter(this, analyzer, create);
  }

  public InstantiatedIndexReader indexReaderFactory() throws IOException {
    return new InstantiatedIndexReader(this);
  }

  public void close() throws IOException {
    // todo: decouple everything
  }

  InstantiatedTerm findTerm(Term term) {
    return findTerm(term.field(), term.text());
  }

  InstantiatedTerm findTerm(String field, String text) {
    Map<String, InstantiatedTerm> termsByField = termsByFieldAndText.get(field);
    if (termsByField == null) {
      return null;
    } else {
      return termsByField.get(text);
    }
  }

  public Map<String, Map<String, InstantiatedTerm>> getTermsByFieldAndText() {
    return termsByFieldAndText;
  }

  public InstantiatedTerm[] getOrderedTerms() {
    return orderedTerms;
  }

  public InstantiatedDocument[] getDocumentsByNumber() {
    return documentsByNumber;
  }

  public Map<String, byte[]> getNormsByFieldNameAndDocumentNumber() {
    return normsByFieldNameAndDocumentNumber;
  }

  void setNormsByFieldNameAndDocumentNumber(Map<String, byte[]> normsByFieldNameAndDocumentNumber) {
    this.normsByFieldNameAndDocumentNumber = normsByFieldNameAndDocumentNumber;
  }

  public BitVector getDeletedDocuments() {
    return deletedDocuments;
  }

  void setDeletedDocuments(BitVector deletedDocuments) {
    this.deletedDocuments = deletedDocuments;
  }

  void setOrderedTerms(InstantiatedTerm[] orderedTerms) {
    this.orderedTerms = orderedTerms;
  }

  void setDocumentsByNumber(InstantiatedDocument[] documentsByNumber) {
    this.documentsByNumber = documentsByNumber;
  }

  public long getVersion() {
    return version;
  }

  void setVersion(long version) {
    this.version = version;
  }

  FieldSettings getFieldSettings() {
    return fieldSettings;
  }
}
