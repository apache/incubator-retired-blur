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
package org.apache.blur.lucene.security.index;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The current {@link SecureAtomicReader} will protect access to documents based
 * on the {@link AccessControl} object.
 * 
 * NOTE: If you are using the {@link Fields} and {@link Terms} with
 * {@link TermsEnum} to create a type ahead. Make sure that you check that the
 * {@link TermsEnum} actually points to a single document because the
 * {@link SecureAtomicReader} will leak terms that users don't have access to
 * read or discover.
 */
public class SecureAtomicReader extends FilterAtomicReader {

  private final AccessControlReader _accessControl;
  private final AtomicReader _original;

  public static SecureAtomicReader create(AccessControlFactory accessControlFactory, AtomicReader in,
      Collection<String> readAuthorizations, Collection<String> discoverAuthorizations, Set<String> discoverableFields)
      throws IOException {
    AccessControlReader accessControlReader = accessControlFactory.getReader(readAuthorizations,
        discoverAuthorizations, discoverableFields);
    return new SecureAtomicReader(in, accessControlReader);
  }

  public SecureAtomicReader(AtomicReader in, AccessControlReader accessControlReader) throws IOException {
    super(in);
    _accessControl = accessControlReader.clone(in);
    _original = in;
  }

  public AtomicReader getOriginalReader() {
    return _original;
  }

  @Override
  public Bits getLiveDocs() {
    final Bits liveDocs = in.getLiveDocs();
    final int maxDoc = maxDoc();
    return new Bits() {

      @Override
      public boolean get(int index) {
        if (liveDocs == null || liveDocs.get(index)) {
          // Need to check access
          try {
            if (_accessControl.hasAccess(ReadType.LIVEDOCS, index)) {
              return true;
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return false;
      }

      @Override
      public int length() {
        return maxDoc;
      }

    };
  }

  @Override
  public Fields getTermVectors(int docID) throws IOException {
    // use doc auth
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void document(int docID, final StoredFieldVisitor visitor) throws IOException {
    if (_accessControl.hasAccess(ReadType.DOCUMENT_FETCH_READ, docID)) {
      GetReadMaskFields getReadMaskFields = new GetReadMaskFields();
      in.document(docID, getReadMaskFields);
      Set<String> readMaskFields = getReadMaskFields.getReadMaskFields();
      if (readMaskFields.isEmpty()) {
        in.document(docID, visitor);
      } else {
        in.document(docID, new ReadMaskStoredFieldVisitor(visitor, readMaskFields));
      }
      return;
    }
    if (_accessControl.hasAccess(ReadType.DOCUMENT_FETCH_DISCOVER, docID)) {
      in.document(docID, new StoredFieldVisitor() {
        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
          if (_accessControl.canDiscoverField(fieldInfo.name)) {
            return visitor.needsField(fieldInfo);
          } else {
            return Status.NO;
          }
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
          visitor.binaryField(fieldInfo, value);
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) throws IOException {
          visitor.stringField(fieldInfo, value);
        }

        @Override
        public void intField(FieldInfo fieldInfo, int value) throws IOException {
          visitor.intField(fieldInfo, value);
        }

        @Override
        public void longField(FieldInfo fieldInfo, long value) throws IOException {
          visitor.longField(fieldInfo, value);
        }

        @Override
        public void floatField(FieldInfo fieldInfo, float value) throws IOException {
          visitor.floatField(fieldInfo, value);
        }

        @Override
        public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
          visitor.doubleField(fieldInfo, value);
        }

      });
      return;
    }
  }

  private static class ReadMaskStoredFieldVisitor extends StoredFieldVisitor {

    private final StoredFieldVisitor _visitor;
    private final Set<String> _readMaskFields;

    public ReadMaskStoredFieldVisitor(StoredFieldVisitor visitor, Set<String> readMaskFields) {
      _visitor = visitor;
      _readMaskFields = readMaskFields;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      if (_readMaskFields.contains(fieldInfo.name)) {
        return Status.NO;
      }
      return Status.YES;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      _visitor.binaryField(fieldInfo, value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      _visitor.stringField(fieldInfo, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      _visitor.intField(fieldInfo, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      _visitor.longField(fieldInfo, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      _visitor.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      _visitor.doubleField(fieldInfo, value);
    }

  }

  private static class GetReadMaskFields extends StoredFieldVisitor {

    private Set<String> _fields = new HashSet<String>();

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      if (fieldInfo.name.equals(FilterAccessControlFactory.READ_MASK_FIELD)) {
        return Status.YES;
      }
      return Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      _fields.add(value);
    }

    Set<String> getReadMaskFields() {
      return _fields;
    }

  }

  @Override
  public Fields fields() throws IOException {
    return new SecureFields(in.fields(), _accessControl, maxDoc());
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    return secureNumericDocValues(in.getNumericDocValues(field), ReadType.NUMERIC_DOC_VALUE);
  }

  private NumericDocValues secureNumericDocValues(final NumericDocValues numericDocValues, final ReadType type) {
    if (numericDocValues == null) {
      return null;
    }
    return new NumericDocValues() {

      @Override
      public long get(int docID) {
        try {
          if (_accessControl.hasAccess(type, docID)) {
            return numericDocValues.get(docID);
          }
          return 0L; // Default missing value.
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    final BinaryDocValues binaryDocValues = in.getBinaryDocValues(field);
    if (binaryDocValues == null) {
      return null;
    }
    return new BinaryDocValues() {

      @Override
      public void get(int docID, BytesRef result) {
        try {
          if (_accessControl.hasAccess(ReadType.BINARY_DOC_VALUE, docID)) {
            binaryDocValues.get(docID, result);
            return;
          }
          // Default missing value.
          result.bytes = MISSING;
          result.length = 0;
          result.offset = 0;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    final SortedDocValues sortedDocValues = in.getSortedDocValues(field);
    if (sortedDocValues == null) {
      return null;
    }
    return new SortedDocValues() {

      @Override
      public void lookupOrd(int ord, BytesRef result) {
        sortedDocValues.lookupOrd(ord, result);
      }

      @Override
      public int getValueCount() {
        return sortedDocValues.getValueCount();
      }

      @Override
      public int getOrd(int docID) {
        try {
          if (_accessControl.hasAccess(ReadType.SORTED_DOC_VALUE, docID)) {
            return sortedDocValues.getOrd(docID);
          }
          return -1; // Default missing value.
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    final SortedSetDocValues sortedSetDocValues = in.getSortedSetDocValues(field);
    if (sortedSetDocValues == null) {
      return null;
    }
    return new SortedSetDocValues() {

      private boolean _access;

      @Override
      public void setDocument(int docID) {
        try {
          if (_access = _accessControl.hasAccess(ReadType.SORTED_SET_DOC_VALUE, docID)) {
            sortedSetDocValues.setDocument(docID);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public long nextOrd() {
        if (_access) {
          return sortedSetDocValues.nextOrd();
        }
        return NO_MORE_ORDS;
      }

      @Override
      public void lookupOrd(long ord, BytesRef result) {
        if (_access) {
          sortedSetDocValues.lookupOrd(ord, result);
        } else {
          result.bytes = BinaryDocValues.MISSING;
          result.length = 0;
          result.offset = 0;
        }
      }

      @Override
      public long getValueCount() {
        return sortedSetDocValues.getValueCount();
      }
    };
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    return secureNumericDocValues(in.getNormValues(field), ReadType.NORM_VALUE);
  }

  static class SecureFields extends FilterFields {

    private final int _maxDoc;
    private final AccessControlReader _accessControlReader;

    public SecureFields(Fields in, AccessControlReader accessControlReader, int maxDoc) {
      super(in);
      _accessControlReader = accessControlReader;
      _maxDoc = maxDoc;
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      Terms readMask = getReadMaskTerms(in, field);
      SecureTerms secureTerms = new SecureTerms(terms, _accessControlReader, _maxDoc);
      if (readMask == null) {
        return secureTerms;
      } else {
        return new ReadMaskTerms(secureTerms, readMask);
      }
    }

    private Terms getReadMaskTerms(Fields in, String field) throws IOException {
      return in.terms(field + FilterAccessControlFactory.READ_MASK_SUFFIX);
    }

  }

  static class ReadMaskTerms extends FilterTerms {

    private final Terms _readMask;

    public ReadMaskTerms(Terms in, Terms readMask) {
      super(in);
      _readMask = readMask;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      TermsEnum maskTermsEnum = _readMask.iterator(null);
      return new ReadMaskTermsEnum(maskTermsEnum, in.iterator(reuse));
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      TermsEnum maskTermsEnum = _readMask.intersect(compiled, startTerm);
      return new ReadMaskTermsEnum(maskTermsEnum, in.intersect(compiled, startTerm));
    }

  }

  static class SecureTerms extends FilterTerms {

    private final int _maxDoc;
    private final AccessControlReader _accessControlReader;

    public SecureTerms(Terms in, AccessControlReader accessControlReader, int maxDoc) {
      super(in);
      _accessControlReader = accessControlReader;
      _maxDoc = maxDoc;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new SecureTermsEnum(in.iterator(reuse), _accessControlReader, _maxDoc);
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new SecureTermsEnum(in.intersect(compiled, startTerm), _accessControlReader, _maxDoc);
    }
  }

  static class ReadMaskTermsEnum extends FilterTermsEnum {

    private final TermsEnum _maskTermsEnum;

    public ReadMaskTermsEnum(TermsEnum maskTermsEnum, TermsEnum realTermsEnum) {
      super(realTermsEnum);
      _maskTermsEnum = maskTermsEnum;
    }

    @Override
    public BytesRef next() throws IOException {
      while (true) {
        BytesRef ref = in.next();
        if (ref == null) {
          return null;
        }
        if (!_maskTermsEnum.seekExact(ref, true)) {
          return ref;
        }
        if (checkDocs()) {
          return ref;
        }
      }
    }

    private boolean checkDocs() throws IOException {
      DocsEnum maskDocsEnum = _maskTermsEnum.docs(null, null, DocsEnum.FLAG_NONE);
      DocsEnum docsEnum = in.docs(null, null, DocsEnum.FLAG_NONE);
      int docId;
      while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (maskDocsEnum.advance(docId) != docId) {
          return true;
        }
      }
      return false;
    }
  }

  static class SecureTermsEnum extends FilterTermsEnum {

    private final int _maxDoc;
    private final AccessControlReader _accessControlReader;

    public SecureTermsEnum(TermsEnum in, AccessControlReader accessControlReader, int maxDoc) {
      super(in);
      _accessControlReader = accessControlReader;
      _maxDoc = maxDoc;
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef t;
      while ((t = in.next()) != null) {
        if (hasAccess(t)) {
          return t;
        }
      }
      return null;
    }

    private boolean hasAccess(BytesRef term) throws IOException {
      DocsEnum docsEnum = in.docs(null, null, DocsEnum.FLAG_NONE);
      int docId;
      while ((docId = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (_accessControlReader.hasAccess(ReadType.TERMS_ENUM, docId)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      Bits secureLiveDocs = getSecureLiveDocs(liveDocs, _maxDoc, _accessControlReader);
      return in.docs(secureLiveDocs, reuse, flags);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags)
        throws IOException {
      Bits secureLiveDocs = getSecureLiveDocs(liveDocs, _maxDoc, _accessControlReader);
      return in.docsAndPositions(secureLiveDocs, reuse, flags);
    }

  }

  public static Bits getSecureLiveDocs(Bits bits, int maxDoc, final AccessControlReader accessControlReader) {
    final Bits liveDocs;
    if (bits == null) {
      liveDocs = getMatchAll(maxDoc);
    } else {
      liveDocs = bits;
    }
    final int length = liveDocs.length();
    Bits secureLiveDocs = new Bits() {
      @Override
      public boolean get(int index) {
        if (liveDocs.get(index)) {
          try {
            if (accessControlReader.hasAccess(ReadType.DOCS_ENUM, index)) {
              return true;
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return false;
      }

      @Override
      public int length() {
        return length;
      }
    };
    return secureLiveDocs;
  }

  public static Bits getMatchAll(final int length) {
    return new Bits() {

      @Override
      public int length() {
        return length;
      }

      @Override
      public boolean get(int index) {
        return true;
      }
    };
  }
}
