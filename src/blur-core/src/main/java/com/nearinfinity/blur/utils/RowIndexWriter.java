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

import static com.nearinfinity.blur.utils.BlurConstants.*;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;
import static com.nearinfinity.blur.utils.BlurConstants.SEP;

import java.io.IOException;
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
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Row;

public class RowIndexWriter {
    
    private static final Log LOG = LogFactory.getLog(RowIndexWriter.class);

    private static final Field PRIME_DOC_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
    private Field rowIdField = new Field(ROW_ID,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
    private Field recordIdField = new Field(RECORD_ID,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
    private Document document = new Document();
    private BlurAnalyzer analyzer;
    private IndexWriter indexWriter;
    private boolean primeDocSet;
    private StringBuilder builder = new StringBuilder();
    
    public RowIndexWriter(IndexWriter indexWriter, BlurAnalyzer analyzer) {
        this.indexWriter = indexWriter;
        this.analyzer = analyzer;
    }
    
    public synchronized boolean add(Row row) throws IOException {
        if (row == null || row.id == null) {
            throw new NullPointerException();
        }
        setupAdd(row,false);  //append only operation, only used during bulk adds
        if (!addInternal(row)) {
            setupAdd(row,true);  //need to remove the previous incomplete row
            if (!addInternal(row)) {
                return false;
            }
        }
        return true;
    }
    
    public synchronized boolean replace(Row row) throws IOException {
        if (row == null || row.id == null) {
            throw new NullPointerException();
        }
        setupAdd(row,true);
        if (!addInternal(row)) {
            setupAdd(row,true);
            if (!addInternal(row)) {
                return false;
            }
        }
        return true;
    }

    private void setupAdd(Row row, boolean replace) throws IOException {
        rowIdField.setValue(row.id);
        primeDocSet = false;
        if (replace) {
            indexWriter.deleteDocuments(new Term(ROW_ID,row.id));
        }
    }

    private boolean addInternal(Row row) throws IOException {
        for (ColumnFamily columnFamily : row.getColumnFamilies()) {
            if (!replace(columnFamily)) {
                return false;
            }
        }
        return true;
    }

    private boolean replace(ColumnFamily columnFamily) throws IOException {
        Map<String, Set<Column>> columns = columnFamily.records;
        if (columns == null) {
            return true;
        }
        String family = columnFamily.getFamily();
        if (family == null) {
            throw new NullPointerException();
        }
        long oldRamSize = indexWriter.ramSizeInBytes();
        for (String recordId : columns.keySet()) {
            if (recordId == null) {
                continue;
            }
            recordIdField.setValue(recordId);
            document.getFields().clear();
            document.add(rowIdField);
            document.add(recordIdField);
            if (addColumns(columns.get(recordId),family)) {
                if (!primeDocSet) {
                    document.add(PRIME_DOC_FIELD);
                    primeDocSet = true;
                }
                long newRamSize = indexWriter.ramSizeInBytes();
                if (newRamSize < oldRamSize) {
                    LOG.info("Flush occur during writing of row, start over.");
                    return false;
                }
                oldRamSize = newRamSize;
                indexWriter.addDocument(document,analyzer);
            }
        }
        return true;
    }

    private boolean addColumns(Set<Column> set, String columnFamily) {
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
            boolean flag = analyzer.isFullTextField(fieldName);
            INNER:
            for (int i = 0; i < size; i++) {
                String value = values.get(i);
                if (value == null) {
                    continue INNER;
                }
                document.add(new Field(fieldName,value,store,index));
                if (flag) {
                    builder.append(value).append(' ');
                }
            }
        }
        if (builder.length() != 0) {
            String superValue = builder.toString();
            document.add(new Field(SUPER, superValue, Store.NO, Index.ANALYZED_NO_NORMS));
        }
        return true;
    }
    
    private String getFieldName(String columnFamily, String name) {
        return columnFamily + SEP + name;
    }
    
}
