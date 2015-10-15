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
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.blur.lucene.security.DocumentAuthorizations;
import org.apache.blur.lucene.security.document.DocumentVisiblityField;
import org.apache.blur.lucene.security.search.BitSetDocumentVisibilityFilterCacheStrategy;
import org.apache.blur.lucene.security.search.DocumentVisibilityFilter;
import org.apache.blur.lucene.security.search.DocumentVisibilityFilterCacheStrategy;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class FilterAccessControlFactory extends AccessControlFactory {

  public static final String DISCOVER_FIELD = "_discover_";
  public static final String READ_FIELD = "_read_";
  public static final String READ_MASK_FIELD = "_readmask_";
  public static final String READ_MASK_SUFFIX = "$" + READ_MASK_FIELD;

  @Override
  public String getDiscoverFieldName() {
    return DISCOVER_FIELD;
  }

  @Override
  public String getReadFieldName() {
    return READ_FIELD;
  }

  @Override
  public String getReadMaskFieldName() {
    return READ_MASK_FIELD;
  }

  @Override
  public String getReadMaskFieldSuffix() {
    return READ_MASK_SUFFIX;
  }

  @Override
  public AccessControlWriter getWriter() {
    return new FilterAccessControlWriter();
  }

  @Override
  public AccessControlReader getReader(Collection<String> readAuthorizations,
      Collection<String> discoverAuthorizations, Set<String> discoverableFields, String defaultReadMaskMessage) {
    return new FilterAccessControlReader(readAuthorizations, discoverAuthorizations, discoverableFields, defaultReadMaskMessage);
  }

  public static class FilterAccessControlReader extends AccessControlReader {

    private final Set<String> _discoverableFields;
    private final DocumentVisibilityFilter _readDocumentVisibilityFilter;
    private final DocumentVisibilityFilter _discoverDocumentVisibilityFilter;
    private final DocumentVisibilityFilterCacheStrategy _filterCacheStrategy;
    private final String _defaultReadMaskMessage;

    private Bits _readBits;
    private Bits _discoverBits;
    private boolean _noReadAccess;
    private boolean _noDiscoverAccess;
    private DocIdSet _readDocIdSet;
    private DocIdSet _discoverDocIdSet;
    private boolean _isClone;

    public FilterAccessControlReader(Collection<String> readAuthorizations, Collection<String> discoverAuthorizations,
        Set<String> discoverableFields, String defaultReadMaskMessage) {
      this(readAuthorizations, discoverAuthorizations, discoverableFields,
          BitSetDocumentVisibilityFilterCacheStrategy.INSTANCE, defaultReadMaskMessage);
    }

    public FilterAccessControlReader(Collection<String> readAuthorizations, Collection<String> discoverAuthorizations,
        Set<String> discoverableFields, DocumentVisibilityFilterCacheStrategy filterCacheStrategy, String defaultReadMaskMessage) {
      _defaultReadMaskMessage=defaultReadMaskMessage;
      _filterCacheStrategy = filterCacheStrategy;

      if (readAuthorizations == null || readAuthorizations.isEmpty()) {
        _noReadAccess = true;
        _readDocumentVisibilityFilter = null;
      } else {
        _readDocumentVisibilityFilter = new DocumentVisibilityFilter(READ_FIELD, new DocumentAuthorizations(
            readAuthorizations), _filterCacheStrategy);
      }

      if (discoverAuthorizations == null || discoverAuthorizations.isEmpty()) {
        _noDiscoverAccess = true;
        _discoverDocumentVisibilityFilter = null;
      } else {
        _discoverDocumentVisibilityFilter = new DocumentVisibilityFilter(DISCOVER_FIELD, new DocumentAuthorizations(
            discoverAuthorizations), _filterCacheStrategy);
      }
      _discoverableFields = discoverableFields;
    }

    @Override
    protected boolean readAccess(int docID) throws IOException {
      checkClone();
      if (_noReadAccess) {
        return false;
      }
      return _readBits.get(docID);
    }

    private void checkClone() throws IOException {
      if (!_isClone) {
        throw new IOException("No AtomicReader set.");
      }
    }

    @Override
    protected boolean discoverAccess(int docID) throws IOException {
      checkClone();
      if (_noDiscoverAccess) {
        return false;
      }
      return _discoverBits.get(docID);
    }

    @Override
    protected boolean readOrDiscoverAccess(int docID) throws IOException {
      if (readAccess(docID)) {
        return true;
      } else {
        return discoverAccess(docID);
      }
    }

    @Override
    public boolean canDiscoverField(String name) {
      return _discoverableFields.contains(name);
    }

    @Override
    public AccessControlReader clone(AtomicReader in) throws IOException {
      try {
        FilterAccessControlReader filterAccessControlReader = (FilterAccessControlReader) super.clone();
        filterAccessControlReader._isClone = true;
        if (_readDocumentVisibilityFilter == null) {
          filterAccessControlReader._noReadAccess = true;
        } else {
          DocIdSet readDocIdSet = _readDocumentVisibilityFilter.getDocIdSet(in.getContext(), in.getLiveDocs());
          if (readDocIdSet == DocIdSet.EMPTY_DOCIDSET || readDocIdSet == null) {
            filterAccessControlReader._noReadAccess = true;
          } else {
            filterAccessControlReader._readBits = readDocIdSet.bits();
            if (filterAccessControlReader._readBits == null) {
              throw new IOException("Read Bits can not be null.");
            }
          }
          filterAccessControlReader._readDocIdSet = readDocIdSet;
        }

        if (_discoverDocumentVisibilityFilter == null) {
          filterAccessControlReader._noDiscoverAccess = true;
        } else {
          DocIdSet discoverDocIdSet = _discoverDocumentVisibilityFilter.getDocIdSet(in.getContext(), in.getLiveDocs());
          if (discoverDocIdSet == DocIdSet.EMPTY_DOCIDSET || discoverDocIdSet == null) {
            filterAccessControlReader._noDiscoverAccess = true;
          } else {
            filterAccessControlReader._discoverBits = discoverDocIdSet.bits();
            if (filterAccessControlReader._discoverBits == null) {
              throw new IOException("Read Bits can not be null.");
            }
          }
          filterAccessControlReader._discoverDocIdSet = discoverDocIdSet;
        }
        return filterAccessControlReader;
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Filter getQueryFilter() throws IOException {
      return new Filter() {
        @Override
        public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
          FilterAccessControlReader accessControlReader = (FilterAccessControlReader) FilterAccessControlReader.this
              .clone(context.reader());
          DocIdSet secureDocIdSet = getSecureDocIdSet(accessControlReader);
          if (acceptDocs == null) {
            return secureDocIdSet;
          } else {
            return applyDeletes(acceptDocs, secureDocIdSet);
          }
        }

        private DocIdSet getSecureDocIdSet(FilterAccessControlReader accessControlReader) throws IOException {
          DocIdSet readDocIdSet = accessControlReader._readDocIdSet;
          DocIdSet discoverDocIdSet = accessControlReader._discoverDocIdSet;
          if (isEmptyOrNull(discoverDocIdSet) && isEmptyOrNull(readDocIdSet)) {
            return DocIdSet.EMPTY_DOCIDSET;
          } else if (isEmptyOrNull(discoverDocIdSet)) {
            return readDocIdSet;
          } else if (isEmptyOrNull(readDocIdSet)) {
            return discoverDocIdSet;
          } else {
            return DocumentVisibilityFilter.getLogicalOr(readDocIdSet, discoverDocIdSet);
          }
        }

        private boolean isEmptyOrNull(DocIdSet docIdSet) {
          if (docIdSet == null || docIdSet == DocIdSet.EMPTY_DOCIDSET) {
            return true;
          }
          return false;
        }
      };
    }

    protected DocIdSet applyDeletes(final Bits acceptDocs, final DocIdSet secureDocIdSet) {
      return new DocIdSet() {

        @Override
        public DocIdSetIterator iterator() throws IOException {
          final DocIdSetIterator docIdSetIterator = secureDocIdSet.iterator();
          return new DocIdSetIterator() {

            @Override
            public int nextDoc() throws IOException {
              int docId;
              while ((docId = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (acceptDocs.get(docId)) {
                  return docId;
                }
              }
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
              int docId = docIdSetIterator.advance(target);
              if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                return DocIdSetIterator.NO_MORE_DOCS;
              }
              if (acceptDocs.get(docId)) {
                return docId;
              }
              while ((docId = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (acceptDocs.get(docId)) {
                  return docId;
                }
              }
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int docID() {
              return docIdSetIterator.docID();
            }

            @Override
            public long cost() {
              return docIdSetIterator.cost() + 1;
            }

          };
        }
      };
    }

    @Override
    public String getDefaultReadMaskMessage() {
      return _defaultReadMaskMessage;
    }
  }

  public static class FilterAccessControlWriter extends AccessControlWriter {

    @Override
    public Iterable<? extends IndexableField> addReadVisiblity(String read, Iterable<? extends IndexableField> fields) {
      return addField(fields, new DocumentVisiblityField(READ_FIELD, read, Store.YES));
    }

    @Override
    public Iterable<? extends IndexableField> addDiscoverVisiblity(String discover, Iterable<? extends IndexableField> fields) {
      return addField(fields, new DocumentVisiblityField(DISCOVER_FIELD, discover, Store.YES));
    }

    @Override
    public Iterable<? extends IndexableField> addReadMask(String fieldToMask, Iterable<? extends IndexableField> fields) {
      return addField(fields, new StoredField(READ_MASK_FIELD, fieldToMask));
    }

    @Override
    public Iterable<? extends IndexableField> lastStepBeforeIndexing(Iterable<? extends IndexableField> fields) {
      return processFieldMasks(fields);
    }

    public static Iterable<? extends IndexableField> processFieldMasks(Iterable<? extends IndexableField> fields) {
      Set<String> fieldsToMask = getFieldsToMask(fields);
      if (fieldsToMask.isEmpty()) {
        return fields;
      }
      List<IndexableField> result = new ArrayList<IndexableField>();
      for (IndexableField field : fields) {
        IndexableFieldType fieldType = field.fieldType();
        // If field is to be indexed and is to be read masked.
        if (fieldsToMask.contains(field.name()) && fieldType.indexed()) {
          // If field is a doc value, then don't bother indexing.
          if (!isDocValue(field)) {
            if (isStoredField(field)) {
              // Stored fields are not indexed, and the document fetch check
              // handles the mask.
              result.add(field);
            } else {
              IndexableField mask = createMaskField(field);
              result.add(field);
              result.add(mask);
            }
          }
        } else {
          result.add(field);
        }
      }
      return result;
    }

    private static Set<String> getFieldsToMask(Iterable<? extends IndexableField> fields) {
      Set<String> result = new HashSet<String>();
      for (IndexableField field : fields) {
        if (field.name().equals(READ_MASK_FIELD)) {
          result.add(getFieldNameOnly(field.stringValue()));
        }
      }
      return result;
    }

    private static String getFieldNameOnly(String s) {
      // remove any stored messages
      int indexOf = s.indexOf('|');
      if (indexOf < 0) {
        return s;
      } else {
        return s.substring(0, indexOf);
      }
    }

    private static boolean isStoredField(IndexableField field) {
      if (field instanceof StoredField) {
        return true;
      }
      return false;
    }

    private static IndexableField createMaskField(IndexableField field) {
      FieldType fieldTypeNotStored = getFieldTypeNotStored(field);
      String name = field.name() + READ_MASK_SUFFIX;
      if (field instanceof DoubleField) {
        DoubleField f = (DoubleField) field;
        return new DoubleField(name, (double) f.numericValue(), fieldTypeNotStored);
      } else if (field instanceof FloatField) {
        FloatField f = (FloatField) field;
        return new FloatField(name, (float) f.numericValue(), fieldTypeNotStored);
      } else if (field instanceof IntField) {
        IntField f = (IntField) field;
        return new IntField(name, (int) f.numericValue(), fieldTypeNotStored);
      } else if (field instanceof LongField) {
        LongField f = (LongField) field;
        return new LongField(name, (long) f.numericValue(), fieldTypeNotStored);
      } else if (field instanceof StringField) {
        StringField f = (StringField) field;
        return new StringField(name, f.stringValue(), Store.NO);
      } else if (field instanceof StringField) {
        TextField f = (TextField) field;
        Reader readerValue = f.readerValue();
        if (readerValue != null) {
          return new TextField(name, readerValue);
        }
        TokenStream tokenStreamValue = f.tokenStreamValue();
        if (tokenStreamValue != null) {
          return new TextField(name, tokenStreamValue);
        }
        return new TextField(name, f.stringValue(), Store.NO);
      } else if (field.getClass().equals(Field.class)) {
        Field f = (Field) field;
        String stringValue = f.stringValue();
        if (stringValue != null) {
          return new Field(name, stringValue, fieldTypeNotStored);
        }
        BytesRef binaryValue = f.binaryValue();
        if (binaryValue != null) {
          return new Field(name, binaryValue, fieldTypeNotStored);
        }
        Number numericValue = f.numericValue();
        if (numericValue != null) {
          throw new RuntimeException("Field [" + field + "] with type [" + field.getClass() + "] is not supported.");
        }
        Reader readerValue = f.readerValue();
        if (readerValue != null) {
          return new Field(name, readerValue, fieldTypeNotStored);
        }
        TokenStream tokenStreamValue = f.tokenStreamValue();
        if (tokenStreamValue != null) {
          return new Field(name, tokenStreamValue, fieldTypeNotStored);
        }
        throw new RuntimeException("Field [" + field + "] with type [" + field.getClass() + "] is not supported.");
      } else {
        throw new RuntimeException("Field [" + field + "] with type [" + field.getClass() + "] is not supported.");
      }
    }

    private static FieldType getFieldTypeNotStored(IndexableField indexableField) {
      Field field = (Field) indexableField;
      FieldType fieldType = field.fieldType();
      FieldType result = new FieldType(fieldType);
      result.setStored(false);
      result.freeze();
      return result;
    }

    private static boolean isDocValue(IndexableField field) {
      if (field instanceof BinaryDocValuesField) {
        return true;
      } else if (field instanceof NumericDocValuesField) {
        return true;
      } else if (field instanceof SortedDocValuesField) {
        return true;
      } else if (field instanceof SortedSetDocValuesField) {
        return true;
      } else {
        return false;
      }
    }
  }

}
