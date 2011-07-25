/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;
import static com.nearinfinity.blur.utils.BlurConstants.SEP;
import static com.nearinfinity.blur.utils.BlurConstants.SUPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Row;

public class RowIndexWriter {
    
//    private static final Log LOG = LogFactory.getLog(RowIndexWriter.class);

    private static final Field PRIME_DOC_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
    private BlurAnalyzer _analyzer;
    private IndexWriter _indexWriter;
    private boolean primeDocSet;
    private StringBuilder builder = new StringBuilder();
    
    public RowIndexWriter(IndexWriter indexWriter, BlurAnalyzer analyzer) {
        _indexWriter = indexWriter;
        _analyzer = analyzer;
    }
    
    public synchronized void add(Row row) throws IOException {
        if (row == null || row.id == null) {
            throw new NullPointerException();
        }
        append(row,false);
    }
    
    public synchronized void replace(Row row) throws IOException {
        if (row == null || row.id == null) {
            throw new NullPointerException();
        }
        append(row,true);
    }

    private void append(Row row, boolean replace) throws IOException {
        primeDocSet = false;
        List<Document> documents = new ArrayList<Document>();
        for (ColumnFamily columnFamily : row.getColumnFamilies()) {
            convert(row.id,columnFamily,documents);
        }
        synchronized (_indexWriter) {
            if (replace) {
                _indexWriter.updateDocuments(new Term(ROW_ID,row.id),documents,_analyzer);
            } else {
                _indexWriter.addDocuments(documents,_analyzer);
            }
        }
    }

    private void convert(String rowId, ColumnFamily columnFamily, List<Document> documents) throws IOException {
        Map<String, Set<Column>> columns = columnFamily.records;
        if (columns == null) {
            return;
        }
        String family = columnFamily.getFamily();
        if (family == null) {
            throw new NullPointerException();
        }
        for (String recordId : columns.keySet()) {
            if (recordId == null) {
                continue;
            }
            Document document = new Document();
            document.add(new Field(ROW_ID,rowId,Store.YES,Index.NOT_ANALYZED_NO_NORMS));
            document.add(new Field(RECORD_ID,recordId,Store.YES,Index.NOT_ANALYZED_NO_NORMS));
            if (addColumns(document, _analyzer, builder, family, columns.get(recordId))) {
                if (!primeDocSet) {
                    document.add(PRIME_DOC_FIELD);
                    primeDocSet = true;
                }
                documents.add(document);
            }
        }
    }

    public static boolean addColumns(Document document, BlurAnalyzer analyzer, StringBuilder builder, String columnFamily, Iterable<Column> set) {
        if (set == null) {
            return false;
        }
        builder.setLength(0);
        OUTER:
        for (Column column : set) {
            String name = column.getName();
            List<String> values = column.values;
            if (values == null || name == null) {
                continue OUTER;
            }
            int size = values.size();
            String fieldName = getFieldName(columnFamily,name);
            Store store = analyzer.getStore(fieldName);
            Index index = analyzer.getIndex(fieldName);
            boolean fullText = analyzer.isFullTextField(fieldName);
            Set<String> subFieldNames = analyzer.getSubIndexNames(fieldName);
            INNER:
            for (int i = 0; i < size; i++) {
                String value = values.get(i);
                if (value == null) {
                    continue INNER;
                }
                document.add(new Field(fieldName,value,store,index));
                if (fullText) {
                    builder.append(value).append(' ');
                }
                if (subFieldNames != null) {
                    for (String subFieldName : subFieldNames) {
                        document.add(new Field(subFieldName,value,Store.NO,index));
                    }
                }
            }
        }
        if (builder.length() != 0) {
            String superValue = builder.toString();
            document.add(new Field(SUPER, superValue, Store.NO, Index.ANALYZED_NO_NORMS));
        }
        return true;
    }
    
    public static String getFieldName(String columnFamily, String name) {
        return columnFamily + SEP + name;
    }
    
}
