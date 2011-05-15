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

package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;
import static com.nearinfinity.blur.utils.RowDocumentUtil.getColumns;
import static com.nearinfinity.blur.utils.RowDocumentUtil.getRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.io.BytesWritable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.concurrent.ExecutorsDynamicConfig;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.search.BlurSearcher;
import com.nearinfinity.blur.lucene.search.FacetQuery;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.results.BlurResultIterableSearcher;
import com.nearinfinity.blur.manager.results.MergerBlurResultIterable;
import com.nearinfinity.blur.manager.status.QueryStatus;
import com.nearinfinity.blur.manager.status.QueryStatusManager;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.thrift.BException;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.FetchRowResult;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.PrimeDocCache;
import com.nearinfinity.blur.utils.TermDocIterable;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class IndexManager {

    private static final String NOT_FOUND = "NOT_FOUND";
    private static final Version LUCENE_VERSION = Version.LUCENE_30;
    private static final Log LOG = LogFactory.getLog(IndexManager.class);
    private static final int MAX_CLAUSE_COUNT = Integer.getInteger("blur.max.clause.count", 1024 * 128);

    private IndexServer indexServer;
    private ExecutorService executor;
    private int threadCount = 32;
    private QueryStatusManager statusManager = new QueryStatusManager();
    private boolean closed;
    private BlurPartitioner<BytesWritable, Void> blurPartitioner = new BlurPartitioner<BytesWritable, Void>();
    private ExecutorsDynamicConfig dynamicConfig;

    public IndexManager() {
        BooleanQuery.setMaxClauseCount(MAX_CLAUSE_COUNT);
    }

    public void init() {
        executor = Executors.newThreadPool("index-manager",threadCount,dynamicConfig);
        statusManager.init();
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;
            statusManager.close();
            executor.shutdownNow();
            indexServer.close();
        }
    }

    public void replaceRow(String table, Row row) throws BlurException {
        throw new RuntimeException("not implemented");
    }

    public void removeRow(String table, String id) throws BlurException {
        throw new RuntimeException("not implemented");
    }

    public void fetchRow(String table, Selector selector, FetchResult fetchResult) throws BlurException {
        validSelector(selector);
        BlurIndex index;
        try {
            if (selector.getLocationId() == null) {
                populateSelector(table,selector);
            }
            String locationId = selector.getLocationId();
            if (locationId.equals(NOT_FOUND)) {
                fetchResult.setDeleted(false);
                fetchResult.setExists(false);
                return;
            }
            String shard = getShard(locationId);
            Map<String, BlurIndex> blurIndexes = indexServer.getIndexes(table);
            if (blurIndexes == null) {
                LOG.error("Table [{0}] not found",table);
                throw new BlurException("Table [" + table + "] not found",null);
            }
            index = blurIndexes.get(shard);
            if (index == null) {
                if (index == null) {
                    LOG.error("Shard [{0}] not found in table [{1}]",shard,table);
                    throw new BlurException("Shard [" + shard + "] not found in table [" + table + "]",null);
                }
            }
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get the correct index reader for selector [{0}].",e,selector);
            throw new BException(e.getMessage(),e);
        }
        IndexReader reader = null;
        try {
            reader = index.getIndexReader();
            fetchRow(reader, table, selector, fetchResult);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to fetch row.", e);
            throw new BException(e.getMessage(),e);
        } finally {
            if (reader != null) {
                //this will allow for closing of index
                try {
                    reader.decRef();
                } catch (IOException e) {
                    LOG.error("Unknown error trying to call decRef on reader [{0}]",e,reader);
                }
            }
        }
    }
    
    private void populateSelector(String table, Selector selector) throws IOException, BlurException {
        String rowId = selector.rowId;
        String recordId = selector.recordId;
        String shardName = getShardName(table, rowId);
        Map<String, BlurIndex> indexes = indexServer.getIndexes(table);
        BlurIndex blurIndex = indexes.get(shardName);
        if (blurIndex == null) {
            throw new BlurException("Shard [" + shardName + "] is not being servered by this shardserver.",null);
        }
        IndexReader reader = blurIndex.getIndexReader();
        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            BooleanQuery query = new BooleanQuery();
            if (selector.recordOnly) {
                query.add(new TermQuery(new Term(RECORD_ID,recordId)), Occur.MUST);
                query.add(new TermQuery(new Term(ROW_ID,rowId)), Occur.MUST);
            } else {
                query.add(new TermQuery(new Term(ROW_ID,rowId)), Occur.MUST);
                query.add(new TermQuery(new Term(PRIME_DOC,PRIME_DOC_VALUE)), Occur.MUST);
            }
            TopDocs topDocs = searcher.search(query, 1);
            if (topDocs.totalHits > 1) {
                if (selector.recordOnly) {
                    LOG.warn("Rowid [" + rowId + "], recordId [" + recordId +
                    		"] has more than one prime doc that is not deleted.");
                } else {
                    LOG.warn("Rowid [" + rowId + "] has more than one prime doc that is not deleted.");
                }
            }
            if (topDocs.totalHits == 1) {
                selector.setLocationId(shardName + "/" + topDocs.scoreDocs[0].doc);
            } else {
                selector.setLocationId(NOT_FOUND);
            }
        } finally {
            //this will allow for closing of index
            reader.decRef();
        }
    }

    public static void validSelector(Selector selector) throws BlurException {
        String locationId = selector.locationId;
        String rowId = selector.rowId;
        String recordId = selector.recordId;
        boolean recordOnly = selector.recordOnly;
        if (locationId != null) {
            if (recordId != null && rowId != null) {
                throw new BlurException("Invalid selector locationId [" + locationId +
                		"] and recordId [" + recordId +
                		"] and rowId [" + rowId +
                		"] are set, if using locationId rowId and recordId are not needed.",null);
            } else if (recordId != null) {
                throw new BlurException("Invalid selector locationId [" + locationId +
                        "] and recordId [" + recordId +
                        "] sre set, if using locationId recordId is not needed.",null);
            } else if (rowId != null) {
                throw new BlurException("Invalid selector locationId [" + locationId +
                        "] and rowId [" + rowId +
                        "] are set, if using locationId rowId is not needed.",null);
            }
        } else {
            if (rowId != null && recordId != null) {
                if (!recordOnly) {
                    throw new BlurException("Invalid both rowid [" + rowId + 
                            "] and recordId [" + recordId +
                    		"] are set, and recordOnly is set to [false].  " +
                    		"If you want entire row, then remove recordId, if you want record only set recordOnly to [true].",null);
                }
            } else if (recordId != null) {
                throw new BlurException("Invalid recordId [" + recordId +
                		"] is set but rowId is not set.  If rowId is not known then a query will be required.", null);
            }
        }
    }

    /**
     * Location id format is <shard>/luceneid.
     * @param locationId
     * @return
     */
    private String getShard(String locationId) {
        String[] split = locationId.split("\\/");
        if (split.length != 2) {
            throw new IllegalArgumentException("Location id invalid [" + locationId + "]");
        }
        return split[0];
    }

    public BlurResultIterable query(final String table, final BlurQuery blurQuery, AtomicLongArray facetedCounts) throws Exception {
        final QueryStatus status = statusManager.newQueryStatus(table, blurQuery);
        try {
            Map<String, BlurIndex> blurIndexes;
            try {
                blurIndexes = indexServer.getIndexes(table);
            } catch (IOException e) {
                LOG.error("Unknown error while trying to fetch index readers.", e);
                throw new BException(e.getMessage(),e);
            }
            Analyzer analyzer = indexServer.getAnalyzer(table);
            Filter preFilter = parseFilter(table, blurQuery.preSuperFilter, false, ScoreType.CONSTANT, analyzer);
            Filter postFilter = parseFilter(table, blurQuery.postSuperFilter, true, ScoreType.CONSTANT, analyzer);
            Query userQuery = parseQuery(blurQuery.queryStr, blurQuery.superQueryOn, 
                    analyzer, postFilter, preFilter, getScoreType(blurQuery.type));
            final Query facetedQuery = getFacetedQuery(blurQuery,userQuery,facetedCounts, analyzer);
            return ForkJoin.execute(executor, blurIndexes.entrySet(),
                new ParallelCall<Entry<String, BlurIndex>, BlurResultIterable>() {
                    @Override
                    public BlurResultIterable call(Entry<String, BlurIndex> entry) throws Exception {
                        status.attachThread();
                        BlurIndex index = entry.getValue();
                        IndexReader reader = index.getIndexReader();
                        try {
                            String shard = entry.getKey();
                            BlurSearcher searcher = new BlurSearcher(reader, 
                                    PrimeDocCache.getTableCache().getShardCache(table).
                                    getIndexReaderCache(shard));
                            searcher.setSimilarity(indexServer.getSimilarity(table));
                            return new BlurResultIterableSearcher((Query) facetedQuery.clone(), table, shard, searcher, blurQuery.selector);
                        } finally {
                            //this will allow for closing of index
                            reader.decRef();
                            status.deattachThread();
                        }
                    }
                }).merge(new MergerBlurResultIterable(blurQuery));
        } finally {
            status.deattachThread();
            statusManager.removeStatus(status);
        }
    }

    private Query getFacetedQuery(BlurQuery blurQuery, Query userQuery, AtomicLongArray counts, Analyzer analyzer) throws ParseException {
        if (blurQuery.facets == null) {
            return userQuery;
        }
        return new FacetQuery(userQuery,getFacetQueries(blurQuery,analyzer),counts);
    }

    private Query[] getFacetQueries(BlurQuery blurQuery, Analyzer analyzer) throws ParseException {
        int size = blurQuery.facets.size();
        Query[] queries = new Query[size];
        for (int i = 0; i < size; i++) {
            queries[i] = parseQuery(blurQuery.facets.get(i).queryStr, blurQuery.superQueryOn, analyzer, null, null, ScoreType.CONSTANT);
        }
        return queries;
    }

    private ScoreType getScoreType(ScoreType type) {
        if (type == null) {
            return ScoreType.SUPER;
        }
        return type;
    }

    public void cancelQuery(String table, long uuid) {
        statusManager.cancelQuery(table, uuid);
    }

    public List<BlurQueryStatus> currentQueries(String table) {
        return statusManager.currentQueries(table);
    }

    private Filter parseFilter(String table, String filter, boolean superQueryOn, ScoreType scoreType, Analyzer analyzer)
            throws ParseException, BlurException {
        if (filter == null) {
            return null;
        }
        return new QueryWrapperFilter(new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, null, scoreType).parse(filter));
    }

    private Query parseQuery(String query, boolean superQueryOn, Analyzer analyzer, Filter postFilter,
            Filter preFilter, ScoreType scoreType) throws ParseException {
        Query result = new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, preFilter, scoreType).parse(query);
        if (postFilter == null) {
            return result;
        }
        return new FilteredQuery(result, postFilter);
    }

    public static void fetchRow(IndexReader reader, String table, Selector selector, FetchResult fetchResult)
            throws CorruptIndexException, IOException {
        fetchResult.table = table;
        String locationId = selector.locationId;
        int lastSlash = locationId.lastIndexOf('/');
        int docId = Integer.parseInt(locationId.substring(lastSlash + 1));
        if (docId >= reader.maxDoc()) {
            throw new RuntimeException("Location id [" + locationId + "] with docId [" + docId +
            		"] is not valid.");
        }
        if (selector.isRecordOnly()) {
            // select only the row for the given data or location id.
            if (reader.isDeleted(docId)) {
                fetchResult.exists = false;
                fetchResult.deleted = true;
                return;
            } else {
                fetchResult.exists = true;
                fetchResult.deleted = false;
                Document document = reader.document(docId, getFieldSelector(selector));
                fetchResult.recordResult = getColumns(document);
                return;
            }
        } else {
            if (reader.isDeleted(docId)) {
                fetchResult.exists = false;
                fetchResult.deleted = true;
                return;
            } else {
                fetchResult.exists = true;
                fetchResult.deleted = false;
                String rowId = getRowId(reader, docId);
                TermDocs termDocs = reader.termDocs(new Term(ROW_ID, rowId));
                fetchResult.rowResult = new FetchRowResult(getRow(new TermDocIterable(termDocs, reader, getFieldSelector(selector))));
                return;
            }
        }
    }

    private static String getRowId(IndexReader reader, int docId) throws CorruptIndexException, IOException {
        Document document = reader.document(docId, new FieldSelector() {
            private static final long serialVersionUID = 4912420100148752051L;

            @Override
            public FieldSelectorResult accept(String fieldName) {
                if (ROW_ID.equals(fieldName)) {
                    return FieldSelectorResult.LOAD_AND_BREAK;
                }
                return FieldSelectorResult.NO_LOAD;
            }
        });
        return document.get(ROW_ID);
    }

    private static String getColumnName(String fieldName) {
        return fieldName.substring(fieldName.lastIndexOf('.') + 1);
    }

    private static String getColumnFamily(String fieldName) {
        return fieldName.substring(0, fieldName.lastIndexOf('.'));
    }

    private static FieldSelector getFieldSelector(final Selector selector) {
        return new FieldSelector() {
            private static final long serialVersionUID = 4089164344758433000L;

            @Override
            public FieldSelectorResult accept(String fieldName) {
                if (ROW_ID.equals(fieldName)) {
                    return FieldSelectorResult.LOAD;
                }
                if (RECORD_ID.equals(fieldName)) {
                    return FieldSelectorResult.LOAD;
                }
                if (PRIME_DOC.equals(fieldName)) {
                    return FieldSelectorResult.NO_LOAD;
                }
                if (selector.columnFamiliesToFetch == null && selector.columnsToFetch == null) {
                    return FieldSelectorResult.LOAD;
                }
                String columnFamily = getColumnFamily(fieldName);
                if (selector.columnFamiliesToFetch != null) {
                    if (selector.columnFamiliesToFetch.contains(columnFamily)) {
                        return FieldSelectorResult.LOAD;
                    }
                    return FieldSelectorResult.NO_LOAD;
                }
                String columnName = getColumnName(fieldName);
                if (selector.columnsToFetch != null) {
                    Set<String> columns = selector.columnsToFetch.get(columnFamily);
                    if (columns != null && columns.contains(columnName)) {
                        return FieldSelectorResult.LOAD;
                    }
                }
                return FieldSelectorResult.NO_LOAD;
            }
        };
    }

    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }
    
    public long recordFrequency(String table, final String columnFamily, final String columnName, final String value) throws Exception {
        Map<String, BlurIndex> blurIndexes;
        try {
            blurIndexes = indexServer.getIndexes(table);
        } catch (IOException e) {
            LOG.error("Unknown error while trying to fetch index readers.", e);
            throw new BException(e.getMessage(),e);
        }
        return ForkJoin.execute(executor, blurIndexes.entrySet(),
            new ParallelCall<Entry<String, BlurIndex>, Long>() {
                @Override
                public Long call(Entry<String, BlurIndex> input) throws Exception {
                    BlurIndex index = input.getValue();
                    IndexReader reader = index.getIndexReader();
                    try {
                        return recordFrequency(reader,columnFamily,columnName,value);
                    } finally {
                        //this will allow for closing of index
                        reader.decRef();
                    }
                }
        }).merge(new Merger<Long>() {
            @Override
            public Long merge(BlurExecutorCompletionService<Long> service) throws Exception {
                long total = 0;
                while (service.getRemainingCount() > 0) {
                    total += service.take().get();
                }
                return total;
            }
        });
    }

    public List<String> terms(String table, final String columnFamily, final String columnName, final String startWith, final short size) throws Exception {
        Map<String, BlurIndex> blurIndexes;
        try {
            blurIndexes = indexServer.getIndexes(table);
        } catch (IOException e) {
            LOG.error("Unknown error while trying to fetch index readers.", e);
            throw new BException(e.getMessage(),e);
        }
        return ForkJoin.execute(executor, blurIndexes.entrySet(),
            new ParallelCall<Entry<String, BlurIndex>, List<String>>() {
                @Override
                public List<String> call(Entry<String, BlurIndex> input) throws Exception {
                    BlurIndex index = input.getValue();
                    IndexReader reader = index.getIndexReader();
                    try {
                        return terms(reader,columnFamily,columnName,startWith,size);
                    } finally {
                        //this will allow for closing of index
                        reader.decRef();
                    }
                }
        }).merge(new Merger<List<String>>() {
            @Override
            public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws Exception {
                TreeSet<String> terms = new TreeSet<String>();
                while (service.getRemainingCount() > 0) {
                    terms.addAll(service.take().get());
                }
                return new ArrayList<String>(terms).subList(0, Math.min(size, terms.size()));
            }
        });
    }
    
    public static long recordFrequency(IndexReader reader, String columnFamily, String columnName, String value) throws IOException {
        return reader.docFreq(getTerm(columnFamily,columnName,value));
    }

    public static List<String> terms(IndexReader reader, String columnFamily, String columnName, String startWith, short size) throws IOException {
        Term term = getTerm(columnFamily, columnName, startWith);
        String field = term.field();
        List<String> terms = new ArrayList<String>(size);
        TermEnum termEnum = reader.terms(term);
        try {
            do {
                Term currentTerm = termEnum.term();
                if (currentTerm == null) {
                    return terms;
                }
                if (!currentTerm.field().equals(field)) {
                    break;
                }
                terms.add(currentTerm.text());
                if (terms.size() >= size) {
                    return terms;
                }
            } while (termEnum.next());
            return terms;
        } finally {
            termEnum.close();
        }
    }
    
    private static Term getTerm(String columnFamily, String columnName, String value) {
        if (columnName == null) {
            throw new NullPointerException("ColumnName cannot both be null.");
        }
        if (columnFamily == null) {
            return new Term(columnName, value);
        }
        return new Term(columnFamily + "." + columnName, value);
    }

    public Schema schema(String table) throws IOException {
        Schema schema = new Schema().setTable(table);
        schema.columnFamilies = new TreeMap<String, Set<String>>();
        Map<String, BlurIndex> blurIndexes = indexServer.getIndexes(table);
        for (BlurIndex blurIndex : blurIndexes.values()) {
            IndexReader reader = blurIndex.getIndexReader();
            try {
                Collection<String> fieldNames = reader.getFieldNames(FieldOption.ALL);
                for (String fieldName : fieldNames) {
                    int index = fieldName.indexOf('.');
                    if (index > 0) {
                        String columnFamily = fieldName.substring(0, index);
                        String column = fieldName.substring(index + 1);
                        Set<String> set = schema.columnFamilies.get(columnFamily);
                        if (set == null) {
                            set = new TreeSet<String>();
                            schema.columnFamilies.put(columnFamily, set);
                        }
                        set.add(column);
                    }
                }
            } finally {
              //this will allow for closing of index
                reader.decRef();
            }
        }
        return schema;
    }

    public void setStatusCleanupTimerDelay(long delay) {
        statusManager.setStatusCleanupTimerDelay(delay);
    }

    public void mutate(String table, List<RowMutation> mutations) throws BlurException, IOException {
        Map<String,List<Row>> rowMap = new HashMap<String, List<Row>>();
        for (RowMutation mutation : mutations) {
            validateMutation(mutation);
            String shard = getShardName(table, mutation.rowId);
            List<Row> list = rowMap.get(shard);
            if (list == null) {
                list = new ArrayList<Row>();
                rowMap.put(shard, list);
            }
            list.add(toRow(mutation));
        }
        Map<String, BlurIndex> indexes = indexServer.getIndexes(table);
        for (String shard : rowMap.keySet()) {
            BlurIndex blurIndex = indexes.get(shard);
            if (blurIndex == null) {
                throw new BlurException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.",null);
            }
            blurIndex.replaceRow(rowMap.get(shard));
        }
    }

    private String getShardName(String table, String rowId) {
        BytesWritable key = getKey(rowId);
        int numberOfShards = getNumberOfShards(table);
        int partition = blurPartitioner.getPartition(key, null, numberOfShards);
        return BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, partition);
    }
    
    private void validateMutation(RowMutation mutation) {
        String rowId = mutation.rowId;
        if (rowId == null) {
            throw new NullPointerException("Rowid can not be null in mutation.");
        }
    }
    
    private Row toRow(RowMutation mutation) {
        RowMutationType type = mutation.rowMutationType;
        switch (type) {
        case REPLACE_ROW:
            return getRowFromMutations(mutation.rowId,mutation.recordMutations);
        default:
            throw new RuntimeException("Not supported [" + type + "]");
        }
    }

    private Row getRowFromMutations(String id, List<RecordMutation> recordMutations) {
        Row row = new Row().setId(id);
        Map<String,ColumnFamily> columnFamily = new HashMap<String, ColumnFamily>();
        for (RecordMutation mutation : recordMutations) {
            ColumnFamily family = columnFamily.get(mutation.family);
            if (family == null) {
                family = new ColumnFamily();
                family.setFamily(mutation.family);
                columnFamily.put(mutation.family, family);
            }
            switch (mutation.recordMutationType) {
            case REPLACE_ENTIRE_RECORD:
                family.putToRecords(mutation.recordId, mutation.record);
                break;
            default:
                throw new RuntimeException("Not supported [" + mutation.recordMutationType + "]");
            }
        }
        for (ColumnFamily family : columnFamily.values()) {
            row.addToColumnFamilies(family);
        }
        return row;
    }

    private int getNumberOfShards(String table) {
        List<String> list = indexServer.getShardList(table);
        return list.size();
    }

    private BytesWritable getKey(String rowId) {
        return new BytesWritable(rowId.getBytes());
    }

    public void setDynamicConfig(ExecutorsDynamicConfig dynamicConfig) {
        this.dynamicConfig = dynamicConfig;
    }

}
