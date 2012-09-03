package com.nearinfinity.blur.manager;

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
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.RECORD_ID;
import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;
import static com.nearinfinity.blur.utils.BlurUtil.findRecordMutation;
import static com.nearinfinity.blur.utils.BlurUtil.readFilter;
import static com.nearinfinity.blur.utils.BlurUtil.readQuery;
import static com.nearinfinity.blur.utils.RowDocumentUtil.getColumns;
import static com.nearinfinity.blur.utils.RowDocumentUtil.getRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.io.BytesWritable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ReaderUtil;

import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.lucene.EscapeRewrite;
import com.nearinfinity.blur.lucene.search.FacetQuery;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.manager.results.BlurResultIterableSearcher;
import com.nearinfinity.blur.manager.results.MergerBlurResultIterable;
import com.nearinfinity.blur.manager.status.QueryStatus;
import com.nearinfinity.blur.manager.status.QueryStatusManager;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.thrift.BException;
import com.nearinfinity.blur.thrift.MutationHelper;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ExpertQuery;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.FetchRowResult;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.RecordMutationType;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService.Cancel;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.blur.utils.TermDocIterable;

public class IndexManager {

  private static final String NOT_FOUND = "NOT_FOUND";
  private static final Log LOG = LogFactory.getLog(IndexManager.class);

  private IndexServer _indexServer;
  private ExecutorService _executor;
  private ExecutorService _mutateExecutor;
  private int _threadCount;
  private QueryStatusManager _statusManager = new QueryStatusManager();
  private boolean _closed;
  private BlurPartitioner<BytesWritable, Void> _blurPartitioner = new BlurPartitioner<BytesWritable, Void>();
  private BlurFilterCache _filterCache = new DefaultBlurFilterCache();
  private BlurMetrics _blurMetrics;
  private long _defaultParallelCallTimeout = TimeUnit.MINUTES.toMillis(1);

  public void setMaxClauseCount(int maxClauseCount) {
    BooleanQuery.setMaxClauseCount(maxClauseCount);
  }

  public void init() {
    _executor = Executors.newThreadPool("index-manager", _threadCount);
    // @TODO give the mutate it's own thread pool
    _mutateExecutor = Executors.newThreadPool("index-manager-mutate", _threadCount);
    _statusManager.init();
    LOG.info("Init Complete");
  }

  public synchronized void close() {
    if (!_closed) {
      _closed = true;
      _statusManager.close();
      _executor.shutdownNow();
      _mutateExecutor.shutdownNow();
      _indexServer.close();
    }
  }

  public void fetchRow(String table, Selector selector, FetchResult fetchResult) throws BlurException {
    validSelector(selector);
    BlurIndex index;
    try {
      if (selector.getLocationId() == null) {
        populateSelector(table, selector);
      }
      String locationId = selector.getLocationId();
      if (locationId.equals(NOT_FOUND)) {
        fetchResult.setDeleted(false);
        fetchResult.setExists(false);
        return;
      }
      String shard = getShard(locationId);
      Map<String, BlurIndex> blurIndexes = _indexServer.getIndexes(table);
      if (blurIndexes == null) {
        LOG.error("Table [{0}] not found", table);
        throw new BlurException("Table [" + table + "] not found", null);
      }
      index = blurIndexes.get(shard);
      if (index == null) {
        if (index == null) {
          LOG.error("Shard [{0}] not found in table [{1}]", shard, table);
          throw new BlurException("Shard [" + shard + "] not found in table [" + table + "]", null);
        }
      }
    } catch (BlurException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get the correct index reader for selector [{0}].", e, selector);
      throw new BException(e.getMessage(), e);
    }
    IndexReader reader = null;
    try {
      reader = index.getIndexReader();
      fetchRow(reader, table, selector, fetchResult);
      if (_blurMetrics != null) {
        if (fetchResult.rowResult != null) {
          if (fetchResult.rowResult.row != null && fetchResult.rowResult.row.records != null) {
            _blurMetrics.recordReads.addAndGet(fetchResult.rowResult.row.records.size());
          }
          _blurMetrics.rowReads.incrementAndGet();
        } else if (fetchResult.recordResult != null) {
          _blurMetrics.recordReads.incrementAndGet();
        }
      }
    } catch (Exception e) {
      LOG.error("Unknown error while trying to fetch row.", e);
      throw new BException(e.getMessage(), e);
    } finally {
      if (reader != null) {
        // this will allow for closing of index
        try {
          reader.decRef();
        } catch (IOException e) {
          LOG.error("Unknown error trying to call decRef on reader [{0}]", e, reader);
        }
      }
    }
  }

  private void populateSelector(String table, Selector selector) throws IOException, BlurException {
    String rowId = selector.rowId;
    String recordId = selector.recordId;
    String shardName = MutationHelper.getShardName(table, rowId, getNumberOfShards(table), _blurPartitioner);
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    BlurIndex blurIndex = indexes.get(shardName);
    if (blurIndex == null) {
      throw new BlurException("Shard [" + shardName + "] is not being servered by this shardserver.", null);
    }
    IndexReader reader = blurIndex.getIndexReader();
    try {
      IndexSearcher searcher = new IndexSearcher(reader);
      BooleanQuery query = new BooleanQuery();
      if (selector.recordOnly) {
        query.add(new TermQuery(new Term(RECORD_ID, recordId)), Occur.MUST);
        query.add(new TermQuery(new Term(ROW_ID, rowId)), Occur.MUST);
      } else {
        query.add(new TermQuery(new Term(ROW_ID, rowId)), Occur.MUST);
        query.add(new TermQuery(BlurConstants.PRIME_DOC_TERM), Occur.MUST);
      }
      TopDocs topDocs = searcher.search(query, 1);
      if (topDocs.totalHits > 1) {
        if (selector.recordOnly) {
          LOG.warn("Rowid [" + rowId + "], recordId [" + recordId + "] has more than one prime doc that is not deleted.");
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
      // this will allow for closing of index
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
        throw new BlurException("Invalid selector locationId [" + locationId + "] and recordId [" + recordId + "] and rowId [" + rowId
            + "] are set, if using locationId, then rowId and recordId are not needed.", null);
      } else if (recordId != null) {
        throw new BlurException("Invalid selector locationId [" + locationId + "] and recordId [" + recordId + "] sre set, if using locationId recordId is not needed.", null);
      } else if (rowId != null) {
        throw new BlurException("Invalid selector locationId [" + locationId + "] and rowId [" + rowId + "] are set, if using locationId rowId is not needed.", null);
      }
    } else {
      if (rowId != null && recordId != null) {
        if (!recordOnly) {
          throw new BlurException("Invalid both rowid [" + rowId + "] and recordId [" + recordId + "] are set, and recordOnly is set to [false].  "
              + "If you want entire row, then remove recordId, if you want record only set recordOnly to [true].", null);
        }
      } else if (recordId != null) {
        throw new BlurException("Invalid recordId [" + recordId + "] is set but rowId is not set.  If rowId is not known then a query will be required.", null);
      }
    }
  }

  /**
   * Location id format is <shard>/luceneid.
   * 
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
    final AtomicBoolean running = new AtomicBoolean(true);
    final QueryStatus status = _statusManager.newQueryStatus(table, blurQuery, _threadCount, running);
    _blurMetrics.queriesExternal.incrementAndGet();
    try {
      Map<String, BlurIndex> blurIndexes;
      try {
        blurIndexes = _indexServer.getIndexes(table);
      } catch (IOException e) {
        LOG.error("Unknown error while trying to fetch index readers.", e);
        throw new BException(e.getMessage(), e);
      }
      Analyzer analyzer = _indexServer.getAnalyzer(table);
      ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> call;
      if (isSimpleQuery(blurQuery)) {
        SimpleQuery simpleQuery = blurQuery.simpleQuery;
        Filter preFilter = QueryParserUtil.parseFilter(table, simpleQuery.preSuperFilter, false, analyzer, _filterCache);
        Filter postFilter = QueryParserUtil.parseFilter(table, simpleQuery.postSuperFilter, true, analyzer, _filterCache);
        Query userQuery = QueryParserUtil.parseQuery(simpleQuery.queryStr, simpleQuery.superQueryOn, analyzer, postFilter, preFilter, getScoreType(simpleQuery.type));
        Query facetedQuery = getFacetedQuery(blurQuery, userQuery, facetedCounts, analyzer);
        call = new SimpleQueryParallelCall(running, table, status, _indexServer, facetedQuery, blurQuery.selector, _blurMetrics);
      } else {
        Query query = getQuery(blurQuery.expertQuery);
        Filter filter = getFilter(blurQuery.expertQuery);
        Query userQuery;
        if (filter != null) {
          userQuery = new FilteredQuery(query, filter);
        } else {
          userQuery = query;
        }
        Query facetedQuery = getFacetedQuery(blurQuery, userQuery, facetedCounts, analyzer);
        call = new SimpleQueryParallelCall(running, table, status, _indexServer, facetedQuery, blurQuery.selector, _blurMetrics);
      }
      MergerBlurResultIterable merger = new MergerBlurResultIterable(blurQuery);
      return ForkJoin.execute(_executor, blurIndexes.entrySet(), call, new Cancel() {
        @Override
        public void cancel() {
          running.set(false);
        }
      }).merge(merger);
    } finally {
      _statusManager.removeStatus(status);
    }
  }

  private Filter getFilter(ExpertQuery expertQuery) throws BException {
    return readFilter(expertQuery.getFilter());
  }

  private Query getQuery(ExpertQuery expertQuery) throws BException {
    return readQuery(expertQuery.getQuery());
  }

  private boolean isSimpleQuery(BlurQuery blurQuery) {
    if (blurQuery.simpleQuery != null) {
      return true;
    }
    return false;
  }

  private Query getFacetedQuery(BlurQuery blurQuery, Query userQuery, AtomicLongArray counts, Analyzer analyzer) throws ParseException {
    if (blurQuery.facets == null) {
      return userQuery;
    }
    return new FacetQuery(userQuery, getFacetQueries(blurQuery, analyzer), counts);
  }

  private Query[] getFacetQueries(BlurQuery blurQuery, Analyzer analyzer) throws ParseException {
    int size = blurQuery.facets.size();
    Query[] queries = new Query[size];
    for (int i = 0; i < size; i++) {
      queries[i] = QueryParserUtil.parseQuery(blurQuery.facets.get(i).queryStr, blurQuery.simpleQuery.superQueryOn, analyzer, null, null, ScoreType.CONSTANT);
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
    _statusManager.cancelQuery(table, uuid);
  }

  public List<BlurQueryStatus> currentQueries(String table) {
    return _statusManager.currentQueries(table);
  }

  public BlurQueryStatus queryStatus(String table, long uuid) throws BlurException {
    return _statusManager.queryStatus(table, uuid);
  }

  public List<Long> queryStatusIdList(String table) {
    return _statusManager.queryStatusIdList(table);
  }

  public static void fetchRow(IndexReader reader, String table, Selector selector, FetchResult fetchResult) throws CorruptIndexException, IOException {
    fetchResult.table = table;
    String locationId = selector.locationId;
    int lastSlash = locationId.lastIndexOf('/');
    int docId = Integer.parseInt(locationId.substring(lastSlash + 1));
    if (docId >= reader.maxDoc()) {
      throw new RuntimeException("Location id [" + locationId + "] with docId [" + docId + "] is not valid.");
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

  public static FieldSelector getFieldSelector(final Selector selector) {
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
    return _indexServer;
  }

  public void setIndexServer(IndexServer indexServer) {
    this._indexServer = indexServer;
  }

  public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value) throws Exception {
    Map<String, BlurIndex> blurIndexes;
    try {
      blurIndexes = _indexServer.getIndexes(table);
    } catch (IOException e) {
      LOG.error("Unknown error while trying to fetch index readers.", e);
      throw new BException(e.getMessage(), e);
    }
    return ForkJoin.execute(_executor, blurIndexes.entrySet(), new ParallelCall<Entry<String, BlurIndex>, Long>() {
      @Override
      public Long call(Entry<String, BlurIndex> input) throws Exception {
        BlurIndex index = input.getValue();
        IndexReader reader = index.getIndexReader();
        try {
          return recordFrequency(reader, columnFamily, columnName, value);
        } finally {
          // this will allow for closing of index
          reader.decRef();
        }
      }
    }).merge(new Merger<Long>() {
      @Override
      public Long merge(BlurExecutorCompletionService<Long> service) throws BlurException {
        long total = 0;
        while (service.getRemainingCount() > 0) {
          Future<Long> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table, columnFamily, columnName, value);
          total += service.getResultThrowException(future, table, columnFamily, columnName, value);
        }
        return total;
      }
    });
  }

  public List<String> terms(final String table, final String columnFamily, final String columnName, final String startWith, final short size) throws Exception {
    Map<String, BlurIndex> blurIndexes;
    try {
      blurIndexes = _indexServer.getIndexes(table);
    } catch (IOException e) {
      LOG.error("Unknown error while trying to fetch index readers.", e);
      throw new BException(e.getMessage(), e);
    }
    return ForkJoin.execute(_executor, blurIndexes.entrySet(), new ParallelCall<Entry<String, BlurIndex>, List<String>>() {
      @Override
      public List<String> call(Entry<String, BlurIndex> input) throws Exception {
        BlurIndex index = input.getValue();
        IndexReader reader = index.getIndexReader();
        try {
          return terms(reader, columnFamily, columnName, startWith, size);
        } finally {
          // this will allow for closing of index
          reader.decRef();
        }
      }
    }).merge(new Merger<List<String>>() {
      @Override
      public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws BlurException {
        TreeSet<String> terms = new TreeSet<String>();
        while (service.getRemainingCount() > 0) {
          Future<List<String>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table, columnFamily, columnName, startWith, size);
          terms.addAll(service.getResultThrowException(future, table, columnFamily, columnName, startWith, size));
        }
        return new ArrayList<String>(terms).subList(0, Math.min(size, terms.size()));
      }
    });
  }

  public static long recordFrequency(IndexReader reader, String columnFamily, String columnName, String value) throws IOException {
    return reader.docFreq(getTerm(columnFamily, columnName, value));
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
    Map<String, BlurIndex> blurIndexes = _indexServer.getIndexes(table);
    for (BlurIndex blurIndex : blurIndexes.values()) {
      IndexReader reader = blurIndex.getIndexReader();
      try {
        FieldInfos mergedFieldInfos = ReaderUtil.getMergedFieldInfos(reader);
        for (FieldInfo fieldInfo : mergedFieldInfos) {
          String fieldName = fieldInfo.name;
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
        // this will allow for closing of index
        reader.decRef();
      }
    }
    return schema;
  }

  public void setStatusCleanupTimerDelay(long delay) {
    _statusManager.setStatusCleanupTimerDelay(delay);
  }

  public void mutate(final RowMutation mutation) throws BlurException, IOException {
    Future<Void> future = _mutateExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        doMutate(mutation);
        return null;
      }
    });
    try {
      future.get();
    } catch (InterruptedException e) {
      throw new BException("Unknown error during mutation", e);
    } catch (ExecutionException e) {
      throw new BException("Unknown error during mutation", e.getCause());
    }
  }

  public void mutate(final List<RowMutation> mutations) throws BlurException, IOException {
    Future<Void> future = _mutateExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        long s = System.nanoTime();
        doMutates(mutations);
        long e = System.nanoTime();
        LOG.debug("doMutates took [" + (e - s) / 1000000.0 + " ms] to complete");
        return null;
      }
    });
    try {
      future.get();
    } catch (InterruptedException e) {
      throw new BException("Unknown error during mutation", e);
    } catch (ExecutionException e) {
      throw new BException("Unknown error during mutation", e.getCause());
    }
  }

  private void doMutates(List<RowMutation> mutations) throws BlurException, IOException {
    Map<String, List<RowMutation>> map = getMutatesPerTable(mutations);
    for (Entry<String, List<RowMutation>> entry : map.entrySet()) {
      doMutates(entry.getKey(), entry.getValue());
    }
  }

  private void doMutates(final String table, List<RowMutation> mutations) throws IOException, BlurException {
    final Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);

    Map<String, List<RowMutation>> mutationsByShard = new HashMap<String, List<RowMutation>>();

    for (int i = 0; i < mutations.size(); i++) {
      RowMutation mutation = mutations.get(i);
      String shard = MutationHelper.getShardName(table, mutation.rowId, getNumberOfShards(table), _blurPartitioner);
      List<RowMutation> list = mutationsByShard.get(shard);
      if (list == null) {
        list = new ArrayList<RowMutation>();
        mutationsByShard.put(shard, list);
      }
      list.add(mutation);
    }

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (Entry<String, List<RowMutation>> entry : mutationsByShard.entrySet()) {
      final String shard = entry.getKey();
      final List<RowMutation> value = entry.getValue();
      futures.add(_mutateExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          executeMutates(table, shard, indexes, value);
          return null;
        }
      }));
    }

    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new BException("Unknown error during mutation", e);
      } catch (ExecutionException e) {
        throw new BException("Unknown error during mutation", e.getCause());
      }
    }
  }

  private void executeMutates(String table, String shard, Map<String, BlurIndex> indexes, List<RowMutation> mutations) throws BlurException, IOException {
    long s = System.nanoTime();
    boolean waitToBeVisible = false;
    for (int i = 0; i < mutations.size(); i++) {
      RowMutation mutation = mutations.get(i);
      if (mutation.waitToBeVisible) {
        waitToBeVisible = true;
      }
      BlurIndex blurIndex = indexes.get(shard);
      if (blurIndex == null) {
        throw new BlurException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.", null);
      }

      boolean waitVisiblity = false;
      if (i + 1 == mutations.size()) {
        waitVisiblity = waitToBeVisible;
      }
      RowMutationType type = mutation.rowMutationType;
      switch (type) {
      case REPLACE_ROW:
        Row row = MutationHelper.getRowFromMutations(mutation.rowId, mutation.recordMutations);
        blurIndex.replaceRow(waitVisiblity, mutation.wal, row);
        break;
      case UPDATE_ROW:
        doUpdateRowMutation(mutation, blurIndex);
        break;
      case DELETE_ROW:
        blurIndex.deleteRow(waitVisiblity, mutation.wal, mutation.rowId);
        break;
      default:
        throw new RuntimeException("Not supported [" + type + "]");
      }
    }
    long e = System.nanoTime();
    LOG.debug("executeMutates took [" + (e - s) / 1000000.0 + " ms] to complete");
  }

  private Map<String, List<RowMutation>> getMutatesPerTable(List<RowMutation> mutations) {
    Map<String, List<RowMutation>> map = new HashMap<String, List<RowMutation>>();
    for (RowMutation mutation : mutations) {
      String table = mutation.table;
      List<RowMutation> list = map.get(table);
      if (list == null) {
        list = new ArrayList<RowMutation>();
        map.put(table, list);
      }
      list.add(mutation);
    }
    return map;
  }

  private void doMutate(RowMutation mutation) throws BlurException, IOException {
    String table = mutation.table;
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    MutationHelper.validateMutation(mutation);
    String shard = MutationHelper.getShardName(table, mutation.rowId, getNumberOfShards(table), _blurPartitioner);
    BlurIndex blurIndex = indexes.get(shard);
    if (blurIndex == null) {
      throw new BlurException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.", null);
    }

    RowMutationType type = mutation.rowMutationType;
    switch (type) {
    case REPLACE_ROW:
      Row row = MutationHelper.getRowFromMutations(mutation.rowId, mutation.recordMutations);
      blurIndex.replaceRow(mutation.waitToBeVisible, mutation.wal, row);
      break;
    case UPDATE_ROW:
      doUpdateRowMutation(mutation, blurIndex);
      break;
    case DELETE_ROW:
      blurIndex.deleteRow(mutation.waitToBeVisible, mutation.wal, mutation.rowId);
      break;
    default:
      throw new RuntimeException("Not supported [" + type + "]");
    }
  }

  private void doUpdateRowMutation(RowMutation mutation, BlurIndex blurIndex) throws BlurException, IOException {
    FetchResult fetchResult = new FetchResult();
    Selector selector = new Selector();
    selector.setAllowStaleDataIsSet(false);
    selector.setRowId(mutation.rowId);
    fetchRow(mutation.table, selector, fetchResult);
    if (fetchResult.exists) {
      // We will examine the contents of the existing row and add records
      // onto a new replacement row based on the mutation we have been given.
      Row existingRow = fetchResult.rowResult.row;
      Row newRow = new Row().setId(existingRow.id);

      // Create a local copy of the mutation we can modify
      RowMutation mutationCopy = mutation.deepCopy();

      // Match existing records against record mutations. Once a record
      // mutation has been processed, remove it from our local copy.
      for (Record existingRecord : existingRow.records) {
        RecordMutation recordMutation = findRecordMutation(mutationCopy, existingRecord);
        if (recordMutation != null) {
          mutationCopy.recordMutations.remove(recordMutation);
          doUpdateRecordMutation(recordMutation, existingRecord, newRow);
        } else {
          // Copy existing records over to the new row unmodified if there
          // is no matching mutation.
          newRow.addToRecords(existingRecord);
        }
      }

      // Examine all remaining record mutations. For any record replacements
      // we need to create a new record in the table even though an existing
      // record did not match. Record deletions are also ok here since the
      // record is effectively already deleted. Other record mutations are
      // an error and should generate an exception.
      for (RecordMutation recordMutation : mutationCopy.recordMutations) {
        RecordMutationType type = recordMutation.recordMutationType;
        switch (type) {
        case DELETE_ENTIRE_RECORD:
          // do nothing as missing record is already in desired state
          break;
        case APPEND_COLUMN_VALUES:
          throw new BException("Mutation cannot append column values to non-existent record", recordMutation);
        case REPLACE_ENTIRE_RECORD:
          newRow.addToRecords(recordMutation.record);
          break;
        case REPLACE_COLUMNS:
          throw new BException("Mutation cannot replace columns in non-existent record", recordMutation);
        default:
          throw new RuntimeException("Unsupported record mutation type [" + type + "]");
        }
      }

      // Finally, replace the existing row with the new row we have built.
      blurIndex.replaceRow(mutation.waitToBeVisible, mutation.wal, newRow);
    } else {
      throw new BException("Mutation cannot update row that does not exist.", mutation);
    }
  }

  private static void doUpdateRecordMutation(RecordMutation recordMutation, Record existingRecord, Row newRow) {
    Record mutationRecord = recordMutation.record;
    switch (recordMutation.recordMutationType) {
    case DELETE_ENTIRE_RECORD:
      return;
    case APPEND_COLUMN_VALUES:
      for (Column column : mutationRecord.columns) {
        existingRecord.addToColumns(column);
      }
      newRow.addToRecords(existingRecord);
      break;
    case REPLACE_ENTIRE_RECORD:
      newRow.addToRecords(mutationRecord);
      break;
    case REPLACE_COLUMNS:
      Set<String> columnNames = new HashSet<String>();
      for (Column column : mutationRecord.columns) {
        columnNames.add(column.name);
      }

      LOOP: for (Column column : existingRecord.columns) {
        // skip columns in existing record that are contained in the mutation
        // record
        if (columnNames.contains(column.name)) {
          continue LOOP;
        }
        mutationRecord.addToColumns(column);
      }
      newRow.addToRecords(mutationRecord);
      break;
    default:
      break;
    }
  }

  // private boolean isSameRecord(Record existingRecord, Record mutationRecord)
  // {
  // if (existingRecord.recordId.equals(mutationRecord.recordId)) {
  // if (existingRecord.family.equals(mutationRecord.family)) {
  // return true;
  // }
  // }
  // return false;
  // }

  private int getNumberOfShards(String table) {
    return _indexServer.getShardCount(table);
  }

  public static class SimpleQueryParallelCall implements ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> {

    private String _table;
    private QueryStatus _status;
    private IndexServer _indexServer;
    private Query _query;
    private Selector _selector;
    private BlurMetrics _blurMetrics;
    private AtomicBoolean _running;

    public SimpleQueryParallelCall(AtomicBoolean running, String table, QueryStatus status, IndexServer indexServer, Query query, Selector selector, BlurMetrics blurMetrics) {
      _running = running;
      _table = table;
      _status = status;
      _indexServer = indexServer;
      _query = query;
      _selector = selector;
      _blurMetrics = blurMetrics;
    }

    @Override
    public BlurResultIterable call(Entry<String, BlurIndex> entry) throws Exception {
      _status.attachThread();
      try {
        BlurIndex index = entry.getValue();
        IndexReader reader = index.getIndexReader();
        String shard = entry.getKey();
        IndexReader escapeReader = EscapeRewrite.wrap(reader, _running);
        IndexSearcher searcher = new IndexSearcher(escapeReader);
        searcher.setSimilarity(_indexServer.getSimilarity(_table));
        Query rewrite = searcher.rewrite((Query) _query.clone());
        return new BlurResultIterableSearcher(_running, rewrite, _table, shard, searcher, _selector, reader);
      } finally {
        _blurMetrics.queriesInternal.incrementAndGet();
        _status.deattachThread();
      }
    }
  }

  public void setThreadCount(int threadCount) {
    this._threadCount = threadCount;
  }

  public void setBlurMetrics(BlurMetrics blurMetrics) {
    _blurMetrics = blurMetrics;
  }

  public void setFilterCache(BlurFilterCache filterCache) {
    _filterCache = filterCache;
  }

  public void optimize(String table, int numberOfSegmentsPerShard) throws BException {
    Map<String, BlurIndex> blurIndexes;
    try {
      blurIndexes = _indexServer.getIndexes(table);
    } catch (IOException e) {
      LOG.error("Unknown error while trying to fetch index readers.", e);
      throw new BException(e.getMessage(), e);
    }

    Collection<BlurIndex> values = blurIndexes.values();
    for (BlurIndex index : values) {
      try {
        index.optimize(numberOfSegmentsPerShard);
      } catch (IOException e) {
        LOG.error("Unknown error while trying to optimize indexes.", e);
        throw new BException(e.getMessage(), e);
      }
    }
  }

}
