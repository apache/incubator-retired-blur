package com.nearinfinity.blur.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class RowIndexWriter implements BlurConstants {
    
    private static final Log LOG = LogFactory.getLog(RowIndexWriter.class);

    private static final Field PRIME_DOC_FIELD = new Field(PRIME_DOC,PRIME_DOC_VALUE,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
    private Field idField = new Field(ID,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
    private Field superKeyField = new Field(SUPER_KEY,"",Store.YES,Index.NOT_ANALYZED_NO_NORMS);
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
        idField.setValue(row.id);
        indexWriter.deleteDocuments(new Term(ID,row.id));
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
        for (String superKey : columns.keySet()) {
            if (superKey == null) {
                continue;
            }
            superKeyField.setValue(superKey);
            document.getFields().clear();
            document.add(idField);
            document.add(superKeyField);
            if (addColumns(columns.get(superKey),family)) {
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
