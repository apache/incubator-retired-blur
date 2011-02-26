package com.nearinfinity.blur.utils;

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

public class RowIndexWriter implements BlurConstants {
    
    private static final Log LOG = LogFactory.getLog(RowIndexWriter.class);

    private static final Field PRIME_DOC_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
    private Field rowIdField = new Field(ROW_ID,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
    private Field recordIdField = new Field(RECORD_ID,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
    private Document document = new Document();
    private BlurAnalyzer analyzer;
    private IndexWriter indexWriter;
    private boolean primeDocSet;
    
    public RowIndexWriter(IndexWriter indexWriter, BlurAnalyzer analyzer) {
        this.indexWriter = indexWriter;
        this.analyzer = analyzer;
    }
    
    public synchronized void replace(Row row) throws IOException {
        if (row == null || row.id == null) {
            throw new NullPointerException();
        }
        setupReplace(row);
        if (!replaceInternal(row)) {
            setupReplace(row);
            if (!replaceInternal(row)) {
                throw new IOException("SuperDocument too large, try increasing ram buffer size.");
            }
        }
    }

    private void setupReplace(Row row) throws IOException {
        rowIdField.setValue(row.id);
        indexWriter.deleteDocuments(new Term(ROW_ID,row.id));
        primeDocSet = false;
    }

    private boolean replaceInternal(Row row) throws IOException {
        for (ColumnFamily columnFamily : row.getColumnFamilies()) {
            if (!replace(columnFamily)) {
                return false;
            }
        }
        return true;
    }

    private boolean replace(ColumnFamily columnFamily) throws IOException {
        Map<String, Set<Column>> columns = columnFamily.columns;
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
                    LOG.info("Flush occur during writing of super document, start over.");
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
        OUTER:
        for (Column column : set) {
            String name = column.getName();
            List<String> values = column.values;
            if (values == null || name == null) {
                continue OUTER;
            }
            int size = values.size();
            INNER:
            for (int i = 0; i < size; i++) {
                String value = values.get(i);
                if (value == null) {
                    continue INNER;
                }
                String fieldName = getFieldName(columnFamily,name);
                Store store = analyzer.getStore(fieldName);
                Index index = analyzer.getIndex(fieldName);
                document.add(new Field(fieldName,value,store,index));
            }
        }
        return true;
    }
    
    private String getFieldName(String columnFamily, String name) {
        return columnFamily + SEP + name;
    }
    
}
