package org.apache.blur.manager;

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
import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.thrift.util.BlurThriftHelper.findRecordMutation;
import static org.apache.blur.utils.BlurConstants.FAMILY;
import static org.apache.blur.utils.BlurConstants.PRIME_DOC;
import static org.apache.blur.utils.BlurConstants.RECORD_ID;
import static org.apache.blur.utils.BlurConstants.ROW_ID;
import static org.apache.blur.utils.RowDocumentUtil.getRecord;
import static org.apache.blur.utils.RowDocumentUtil.getRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.index.ExitableReader;
import org.apache.blur.index.ExitableReader.ExitingReaderException;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.FacetQuery;
import org.apache.blur.lucene.search.StopExecutionCollector.StopExecutionCollectorException;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.manager.results.BlurResultIterableSearcher;
import org.apache.blur.manager.results.MergerBlurResultIterable;
import org.apache.blur.manager.status.QueryStatus;
import org.apache.blur.manager.status.QueryStatusManager;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardServerContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.MutationHelper;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.QueryState;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.BlurExecutorCompletionService.Cancel;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ForkJoin;
import org.apache.blur.utils.ForkJoin.Merger;
import org.apache.blur.utils.ForkJoin.ParallelCall;
import org.apache.blur.utils.HighlightHelper;
import org.apache.blur.utils.ResetableDocumentStoredFieldVisitor;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class IndexManager {

  private static final String NOT_FOUND = "NOT_FOUND";
  private static final Log LOG = LogFactory.getLog(IndexManager.class);

  private IndexServer _indexServer;
  private ClusterStatus _clusterStatus;
  private ExecutorService _executor;
  private ExecutorService _mutateExecutor;
  private int _threadCount;
  private QueryStatusManager _statusManager = new QueryStatusManager();
  private boolean _closed;
  private BlurPartitioner _blurPartitioner = new BlurPartitioner();
  private BlurFilterCache _filterCache = new DefaultBlurFilterCache();
  private long _defaultParallelCallTimeout = TimeUnit.MINUTES.toMillis(1);
  private Meter _readRecordsMeter;
  private Meter _readRowMeter;
  private Meter _writeRecordsMeter;
  private Meter _writeRowMeter;
  private Meter _queriesExternalMeter;
  private Meter _queriesInternalMeter;
  private Timer _fetchTimer;
  private int _fetchCount = 100;
  private int _maxHeapPerRowFetch = 10000000;

  public static AtomicBoolean DEBUG_RUN_SLOW = new AtomicBoolean(false);

  public void setMaxClauseCount(int maxClauseCount) {
    BooleanQuery.setMaxClauseCount(maxClauseCount);
  }

  public void init() {
    _readRecordsMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Read Records/s"), "Records/s",
        TimeUnit.SECONDS);
    _readRowMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Read Row/s"), "Row/s", TimeUnit.SECONDS);
    _writeRecordsMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Write Records/s"), "Records/s",
        TimeUnit.SECONDS);
    _writeRowMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Write Row/s"), "Row/s", TimeUnit.SECONDS);

    _queriesExternalMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "External Queries/s"),
        "External Queries/s", TimeUnit.SECONDS);
    _queriesInternalMeter = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, BLUR, "Internal Queries/s"),
        "Internal Queries/s", TimeUnit.SECONDS);
    _fetchTimer = Metrics.newTimer(new MetricName(ORG_APACHE_BLUR, BLUR, "Fetch Timer"), TimeUnit.MICROSECONDS,
        TimeUnit.SECONDS);

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
    String shard;
    try {
      if (selector.getLocationId() == null) {
        // Not looking up by location id so we should resetSearchers.
        ShardServerContext.resetSearchers();
        populateSelector(table, selector);
      }
      String locationId = selector.getLocationId();
      if (locationId.equals(NOT_FOUND)) {
        fetchResult.setDeleted(false);
        fetchResult.setExists(false);
        return;
      }
      shard = getShard(locationId);
      Map<String, BlurIndex> blurIndexes = _indexServer.getIndexes(table);
      if (blurIndexes == null) {
        LOG.error("Table [{0}] not found", table);
        // @TODO probably should make a enum for not found on this server so the
        // controller knows to try another server.
        throw new BException("Table [" + table + "] not found");
      }
      index = blurIndexes.get(shard);
      if (index == null) {
        LOG.error("Shard [{0}] not found in table [{1}]", shard, table);
        // @TODO probably should make a enum for not found on this server so the
        // controller knows to try another server.
        throw new BException("Shard [" + shard + "] not found in table [" + table + "]");
      }
    } catch (BlurException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get the correct index reader for selector [{0}].", e, selector);
      throw new BException(e.getMessage(), e);
    }
    IndexSearcherClosable searcher = null;
    TimerContext timerContext = _fetchTimer.time();
    boolean usedCache = true;
    try {
      ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
      if (shardServerContext != null) {
        searcher = shardServerContext.getIndexSearcherClosable(table, shard);
      }
      if (searcher == null) {
        searcher = index.getIndexReader();
        usedCache = false;
      }

      TableContext tableContext = getTableContext(table);
      FieldManager fieldManager = tableContext.getFieldManager();

      Query highlightQuery = getHighlightQuery(selector, table, fieldManager);

      fetchRow(searcher.getIndexReader(), table, shard, selector, fetchResult, highlightQuery, fieldManager,
          _maxHeapPerRowFetch);

      if (fetchResult.rowResult != null) {
        if (fetchResult.rowResult.row != null && fetchResult.rowResult.row.records != null) {
          _readRecordsMeter.mark(fetchResult.rowResult.row.records.size());
        }
        _readRowMeter.mark();
      } else if (fetchResult.recordResult != null) {
        _readRecordsMeter.mark();
      }
    } catch (Exception e) {
      LOG.error("Unknown error while trying to fetch row.", e);
      throw new BException(e.getMessage(), e);
    } finally {
      timerContext.stop();
      if (!usedCache && searcher != null) {
        // if the cached search was not used, close the searcher.
        // this will allow for closing of index
        try {
          searcher.close();
        } catch (IOException e) {
          LOG.error("Unknown error trying to call close on searcher [{0}]", e, searcher);
        }
      }
    }
  }

  private Query getHighlightQuery(Selector selector, String table, FieldManager fieldManager) throws ParseException,
      BlurException {
    HighlightOptions highlightOptions = selector.getHighlightOptions();
    if (highlightOptions == null) {
      return null;
    }
    org.apache.blur.thrift.generated.Query query = highlightOptions.getQuery();
    if (query == null) {
      return null;
    }

    TableContext context = getTableContext(table);
    Filter preFilter = QueryParserUtil.parseFilter(table, query.recordFilter, false, fieldManager, _filterCache,
        context);
    Filter postFilter = QueryParserUtil.parseFilter(table, query.rowFilter, true, fieldManager, _filterCache, context);
    return QueryParserUtil.parseQuery(query.query, query.rowQuery, fieldManager, postFilter, preFilter,
        getScoreType(query.scoreType), context);
  }

  private void populateSelector(String table, Selector selector) throws IOException, BlurException {
    String rowId = selector.rowId;
    String recordId = selector.recordId;
    String shardName = MutationHelper.getShardName(table, rowId, getNumberOfShards(table), _blurPartitioner);
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    BlurIndex blurIndex = indexes.get(shardName);
    if (blurIndex == null) {
      throw new BException("Shard [" + shardName + "] is not being servered by this shardserver.");
    }
    IndexSearcherClosable searcher = blurIndex.getIndexReader();
    try {
      BooleanQuery query = new BooleanQuery();
      if (selector.recordOnly) {
        query.add(new TermQuery(new Term(RECORD_ID, recordId)), Occur.MUST);
        query.add(new TermQuery(new Term(ROW_ID, rowId)), Occur.MUST);
      } else {
        query.add(new TermQuery(new Term(ROW_ID, rowId)), Occur.MUST);
        query.add(new TermQuery(BlurUtil.PRIME_DOC_TERM), Occur.MUST);
      }
      TopDocs topDocs = searcher.search(query, 1);
      if (topDocs.totalHits > 1) {
        if (selector.recordOnly) {
          LOG.warn("Rowid [" + rowId + "], recordId [" + recordId
              + "] has more than one prime doc that is not deleted.");
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
      searcher.close();
    }
  }

  public static void validSelector(Selector selector) throws BlurException {
    String locationId = selector.locationId;
    String rowId = selector.rowId;
    String recordId = selector.recordId;
    boolean recordOnly = selector.recordOnly;

    if (locationId != null) {
      if (recordId != null && rowId != null) {
        throw new BException("Invalid selector locationId [" + locationId + "] and recordId [" + recordId
            + "] and rowId [" + rowId + "] are set, if using locationId, then rowId and recordId are not needed.");
      } else if (recordId != null) {
        throw new BException("Invalid selector locationId [" + locationId + "] and recordId [" + recordId
            + "] sre set, if using locationId recordId is not needed.");
      } else if (rowId != null) {
        throw new BException("Invalid selector locationId [" + locationId + "] and rowId [" + rowId
            + "] are set, if using locationId rowId is not needed.");
      }
    } else {
      if (rowId != null && recordId != null) {
        if (!recordOnly) {
          throw new BException("Invalid both rowid [" + rowId + "] and recordId [" + recordId
              + "] are set, and recordOnly is set to [false].  "
              + "If you want entire row, then remove recordId, if you want record only set recordOnly to [true].");
        }
      } else if (recordId != null) {
        throw new BException("Invalid recordId [" + recordId
            + "] is set but rowId is not set.  If rowId is not known then a query will be required.");
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

  public BlurResultIterable query(final String table, final BlurQuery blurQuery, AtomicLongArray facetedCounts)
      throws Exception {
    boolean runSlow = DEBUG_RUN_SLOW.get();
    final AtomicBoolean running = new AtomicBoolean(true);
    final QueryStatus status = _statusManager.newQueryStatus(table, blurQuery, _threadCount, running);
    _queriesExternalMeter.mark();
    try {
      Map<String, BlurIndex> blurIndexes;
      try {
        blurIndexes = _indexServer.getIndexes(table);
      } catch (IOException e) {
        LOG.error("Unknown error while trying to fetch index readers.", e);
        throw new BException(e.getMessage(), e);
      }
      ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
      ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> call;
      TableContext context = getTableContext(table);
      FieldManager fieldManager = context.getFieldManager();
      org.apache.blur.thrift.generated.Query simpleQuery = blurQuery.query;
      Filter preFilter = QueryParserUtil.parseFilter(table, simpleQuery.recordFilter, false, fieldManager,
          _filterCache, context);
      Filter postFilter = QueryParserUtil.parseFilter(table, simpleQuery.rowFilter, true, fieldManager, _filterCache,
          context);
      Query userQuery = QueryParserUtil.parseQuery(simpleQuery.query, simpleQuery.rowQuery, fieldManager, postFilter,
          preFilter, getScoreType(simpleQuery.scoreType), context);
      Query facetedQuery = getFacetedQuery(blurQuery, userQuery, facetedCounts, fieldManager, context, postFilter,
          preFilter);
      call = new SimpleQueryParallelCall(running, table, status, _indexServer, facetedQuery, blurQuery.selector,
          _queriesInternalMeter, shardServerContext, runSlow, _fetchCount, _maxHeapPerRowFetch);
      MergerBlurResultIterable merger = new MergerBlurResultIterable(blurQuery);
      return ForkJoin.execute(_executor, blurIndexes.entrySet(), call, new Cancel() {
        @Override
        public void cancel() {
          running.set(false);
        }
      }).merge(merger);
    } catch (StopExecutionCollectorException e) {
      BlurQueryStatus queryStatus = status.getQueryStatus();
      QueryState state = queryStatus.getState();
      if (state == QueryState.BACK_PRESSURE_INTERRUPTED) {
        throw new BlurException("Cannot execute query right now.", null, ErrorType.BACK_PRESSURE);
      } else if (state == QueryState.INTERRUPTED) {
        throw new BlurException("Cannot execute query right now.", null, ErrorType.QUERY_CANCEL);
      }
      throw e;
    } catch (ExitingReaderException e) {
      BlurQueryStatus queryStatus = status.getQueryStatus();
      QueryState state = queryStatus.getState();
      if (state == QueryState.BACK_PRESSURE_INTERRUPTED) {
        throw new BlurException("Cannot execute query right now.", null, ErrorType.BACK_PRESSURE);
      } else if (state == QueryState.INTERRUPTED) {
        throw new BlurException("Cannot execute query right now.", null, ErrorType.QUERY_CANCEL);
      }
      throw e;
    } finally {
      _statusManager.removeStatus(status);
    }
  }

  public String parseQuery(String table, org.apache.blur.thrift.generated.Query simpleQuery) throws ParseException,
      BlurException {
    TableContext context = getTableContext(table);
    FieldManager fieldManager = context.getFieldManager();
    Filter preFilter = QueryParserUtil.parseFilter(table, simpleQuery.recordFilter, false, fieldManager, _filterCache,
        context);
    Filter postFilter = QueryParserUtil.parseFilter(table, simpleQuery.rowFilter, true, fieldManager, _filterCache,
        context);
    Query userQuery = QueryParserUtil.parseQuery(simpleQuery.query, simpleQuery.rowQuery, fieldManager, postFilter,
        preFilter, getScoreType(simpleQuery.scoreType), context);
    return userQuery.toString();
  }

  private TableContext getTableContext(final String table) {
    return TableContext.create(_clusterStatus.getTableDescriptor(true, _clusterStatus.getCluster(true, table), table));
  }

  private Query getFacetedQuery(BlurQuery blurQuery, Query userQuery, AtomicLongArray counts,
      FieldManager fieldManager, TableContext context, Filter postFilter, Filter preFilter) throws ParseException {
    if (blurQuery.facets == null) {
      return userQuery;
    }
    return new FacetQuery(userQuery, getFacetQueries(blurQuery, fieldManager, context, postFilter, preFilter), counts);
  }

  private Query[] getFacetQueries(BlurQuery blurQuery, FieldManager fieldManager, TableContext context,
      Filter postFilter, Filter preFilter) throws ParseException {
    int size = blurQuery.facets.size();
    Query[] queries = new Query[size];
    for (int i = 0; i < size; i++) {
      queries[i] = QueryParserUtil.parseQuery(blurQuery.facets.get(i).queryStr, blurQuery.query.rowQuery, fieldManager,
          postFilter, preFilter, ScoreType.CONSTANT, context);
    }
    return queries;
  }

  private ScoreType getScoreType(ScoreType type) {
    if (type == null) {
      return ScoreType.SUPER;
    }
    return type;
  }

  public void cancelQuery(String table, String uuid) {
    _statusManager.cancelQuery(table, uuid);
  }

  public List<BlurQueryStatus> currentQueries(String table) {
    return _statusManager.currentQueries(table);
  }

  public BlurQueryStatus queryStatus(String table, String uuid) {
    return _statusManager.queryStatus(table, uuid);
  }

  public List<String> queryStatusIdList(String table) {
    return _statusManager.queryStatusIdList(table);
  }

  public static void fetchRow(IndexReader reader, String table, String shard, Selector selector,
      FetchResult fetchResult, Query highlightQuery, int maxHeap) throws CorruptIndexException, IOException {
    fetchRow(reader, table, shard, selector, fetchResult, highlightQuery, null, maxHeap);
  }

  public static void fetchRow(IndexReader reader, String table, String shard, Selector selector,
      FetchResult fetchResult, Query highlightQuery, FieldManager fieldManager, int maxHeap)
      throws CorruptIndexException, IOException {
    fetchResult.table = table;
    String locationId = selector.locationId;
    int lastSlash = locationId.lastIndexOf('/');
    int docId = Integer.parseInt(locationId.substring(lastSlash + 1));
    if (docId >= reader.maxDoc()) {
      throw new RuntimeException("Location id [" + locationId + "] with docId [" + docId + "] is not valid.");
    }

    boolean returnIdsOnly = false;
    if (selector.columnFamiliesToFetch != null && selector.columnsToFetch != null
        && selector.columnFamiliesToFetch.isEmpty() && selector.columnsToFetch.isEmpty()) {
      // exit early
      returnIdsOnly = true;
    }

    Bits liveDocs = MultiFields.getLiveDocs(reader);
    ResetableDocumentStoredFieldVisitor fieldVisitor = getFieldSelector(selector);
    if (selector.isRecordOnly()) {
      // select only the row for the given data or location id.
      if (liveDocs != null && !liveDocs.get(docId)) {
        fetchResult.exists = false;
        fetchResult.deleted = true;
        return;
      } else {
        fetchResult.exists = true;
        fetchResult.deleted = false;
        reader.document(docId, fieldVisitor);
        Document document = fieldVisitor.getDocument();
        if (highlightQuery != null && fieldManager != null) {
          HighlightOptions highlightOptions = selector.getHighlightOptions();
          String preTag = highlightOptions.getPreTag();
          String postTag = highlightOptions.getPostTag();
          try {
            document = HighlightHelper
                .highlight(docId, document, highlightQuery, fieldManager, reader, preTag, postTag);
          } catch (InvalidTokenOffsetsException e) {
            LOG.error("Unknown error while tring to highlight", e);
          }
        }
        fieldVisitor.reset();
        fetchResult.recordResult = getRecord(document);
        return;
      }
    } else {
      if (liveDocs != null && !liveDocs.get(docId)) {
        fetchResult.exists = false;
        fetchResult.deleted = true;
        return;
      } else {
        fetchResult.exists = true;
        fetchResult.deleted = false;
        String rowId = getRowId(reader, docId);
        Term term = new Term(ROW_ID, rowId);
        if (returnIdsOnly) {
          int recordCount = BlurUtil.countDocuments(reader, term);
          fetchResult.rowResult = new FetchRowResult();
          fetchResult.rowResult.row = new Row(rowId, null, recordCount);
        } else {
          List<Document> docs;
          if (highlightQuery != null && fieldManager != null) {
            HighlightOptions highlightOptions = selector.getHighlightOptions();
            String preTag = highlightOptions.getPreTag();
            String postTag = highlightOptions.getPostTag();
            docs = HighlightHelper.highlightDocuments(reader, term, fieldVisitor, selector, highlightQuery,
                fieldManager, preTag, postTag);
          } else {
            docs = BlurUtil.fetchDocuments(reader, term, fieldVisitor, selector, maxHeap, table + "/" + shard);
          }
          fetchResult.rowResult = new FetchRowResult(getRow(docs));
        }
        return;
      }
    }
  }

  private static String getRowId(IndexReader reader, int docId) throws CorruptIndexException, IOException {
    reader.document(docId, new StoredFieldVisitor() {
      @Override
      public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (ROW_ID.equals(fieldInfo.name)) {
          return StoredFieldVisitor.Status.STOP;
        }
        return StoredFieldVisitor.Status.NO;
      }
    });
    return reader.document(docId).get(ROW_ID);
  }

  private static String getColumnName(String fieldName) {
    return fieldName.substring(fieldName.lastIndexOf('.') + 1);
  }

  private static String getColumnFamily(String fieldName) {
    return fieldName.substring(0, fieldName.lastIndexOf('.'));
  }

  public static ResetableDocumentStoredFieldVisitor getFieldSelector(final Selector selector) {
    return new ResetableDocumentStoredFieldVisitor() {
      @Override
      public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (ROW_ID.equals(fieldInfo.name)) {
          return StoredFieldVisitor.Status.YES;
        }
        if (RECORD_ID.equals(fieldInfo.name)) {
          return StoredFieldVisitor.Status.YES;
        }
        if (PRIME_DOC.equals(fieldInfo.name)) {
          return StoredFieldVisitor.Status.NO;
        }
        if (FAMILY.equals(fieldInfo.name)) {
          return StoredFieldVisitor.Status.YES;
        }
        if (selector.columnFamiliesToFetch == null && selector.columnsToFetch == null) {
          return StoredFieldVisitor.Status.YES;
        }
        String columnFamily = getColumnFamily(fieldInfo.name);
        if (selector.columnFamiliesToFetch != null) {
          if (selector.columnFamiliesToFetch.contains(columnFamily)) {
            return StoredFieldVisitor.Status.YES;
          }
        }
        String columnName = getColumnName(fieldInfo.name);
        if (selector.columnsToFetch != null) {
          Set<String> columns = selector.columnsToFetch.get(columnFamily);
          if (columns != null && columns.contains(columnName)) {
            return StoredFieldVisitor.Status.YES;
          }
        }
        return StoredFieldVisitor.Status.NO;
      }

    };
  }

  public IndexServer getIndexServer() {
    return _indexServer;
  }

  public void setIndexServer(IndexServer indexServer) {
    this._indexServer = indexServer;
  }

  public long recordFrequency(final String table, final String columnFamily, final String columnName, final String value)
      throws Exception {
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
        IndexSearcherClosable searcher = index.getIndexReader();
        try {
          return recordFrequency(searcher.getIndexReader(), columnFamily, columnName, value);
        } finally {
          // this will allow for closing of index
          searcher.close();
        }
      }
    }).merge(new Merger<Long>() {
      @Override
      public Long merge(BlurExecutorCompletionService<Long> service) throws BlurException {
        long total = 0;
        while (service.getRemainingCount() > 0) {
          Future<Long> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table,
              columnFamily, columnName, value);
          total += service.getResultThrowException(future, table, columnFamily, columnName, value);
        }
        return total;
      }
    });
  }

  public List<String> terms(final String table, final String columnFamily, final String columnName,
      final String startWith, final short size) throws Exception {
    Map<String, BlurIndex> blurIndexes;
    try {
      blurIndexes = _indexServer.getIndexes(table);
    } catch (IOException e) {
      LOG.error("Unknown error while trying to fetch index readers.", e);
      throw new BException(e.getMessage(), e);
    }
    return ForkJoin.execute(_executor, blurIndexes.entrySet(),
        new ParallelCall<Entry<String, BlurIndex>, List<String>>() {
          @Override
          public List<String> call(Entry<String, BlurIndex> input) throws Exception {
            BlurIndex index = input.getValue();
            IndexSearcherClosable searcher = index.getIndexReader();
            try {
              return terms(searcher.getIndexReader(), columnFamily, columnName, startWith, size);
            } finally {
              // this will allow for closing of index
              searcher.close();
            }
          }
        }).merge(new Merger<List<String>>() {
      @Override
      public List<String> merge(BlurExecutorCompletionService<List<String>> service) throws BlurException {
        TreeSet<String> terms = new TreeSet<String>();
        while (service.getRemainingCount() > 0) {
          Future<List<String>> future = service.poll(_defaultParallelCallTimeout, TimeUnit.MILLISECONDS, true, table,
              columnFamily, columnName, startWith, size);
          terms.addAll(service.getResultThrowException(future, table, columnFamily, columnName, startWith, size));
        }
        return new ArrayList<String>(terms).subList(0, Math.min(size, terms.size()));
      }
    });
  }

  public static long recordFrequency(IndexReader reader, String columnFamily, String columnName, String value)
      throws IOException {
    return reader.docFreq(getTerm(columnFamily, columnName, value));
  }

  public static List<String> terms(IndexReader reader, String columnFamily, String columnName, String startWith,
      short size) throws IOException {
    if (startWith == null) {
      startWith = "";
    }
    Term term = getTerm(columnFamily, columnName, startWith);
    List<String> terms = new ArrayList<String>(size);
    AtomicReader areader = BlurUtil.getAtomicReader(reader);
    Terms termsAll = areader.terms(term.field());
    TermsEnum termEnum = termsAll.iterator(null);
    BytesRef currentTermText;
    while ((currentTermText = termEnum.next()) != null) {
      terms.add(currentTermText.utf8ToString());
      if (terms.size() >= size) {
        return terms;
      }
    }
    return terms;
  }

  private static Term getTerm(String columnFamily, String columnName, String value) {
    if (columnName == null) {
      throw new NullPointerException("ColumnName cannot be null.");
    }
    if (columnFamily == null) {
      return new Term(columnName, value);
    }
    return new Term(columnFamily + "." + columnName, value);
  }

  public Schema schema(String table) throws IOException {
    TableContext tableContext = getTableContext(table);
    FieldManager fieldManager = tableContext.getFieldManager();
    Schema schema = new Schema().setTable(table);
    schema.setFamilies(new HashMap<String, Map<String, ColumnDefinition>>());
    Set<String> fieldNames = fieldManager.getFieldNames();
    INNER: for (String fieldName : fieldNames) {
      FieldTypeDefinition fieldTypeDefinition = fieldManager.getFieldTypeDefinition(fieldName);
      if (fieldTypeDefinition == null) {
        continue INNER;
      }
      String columnName = fieldTypeDefinition.getColumnName();
      String columnFamily = fieldTypeDefinition.getFamily();
      String subColumnName = fieldTypeDefinition.getSubColumnName();
      Map<String, ColumnDefinition> map = schema.getFamilies().get(columnFamily);
      if (map == null) {
        map = new HashMap<String, ColumnDefinition>();
        schema.putToFamilies(columnFamily, map);
      }
      if (subColumnName == null) {
        map.put(columnName, getColumnDefinition(fieldTypeDefinition));
      } else {
        map.put(columnName + "." + subColumnName, getColumnDefinition(fieldTypeDefinition));
      }
    }
    return schema;
  }

  private static ColumnDefinition getColumnDefinition(FieldTypeDefinition fieldTypeDefinition) {
    ColumnDefinition columnDefinition = new ColumnDefinition();
    columnDefinition.setFamily(fieldTypeDefinition.getFamily());
    columnDefinition.setColumnName(fieldTypeDefinition.getColumnName());
    columnDefinition.setSubColumnName(fieldTypeDefinition.getSubColumnName());
    columnDefinition.setFieldLessIndexed(fieldTypeDefinition.isFieldLessIndexed());
    columnDefinition.setFieldType(fieldTypeDefinition.getFieldType());
    columnDefinition.setProperties(fieldTypeDefinition.getProperties());
    return columnDefinition;
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

  private void executeMutates(String table, String shard, Map<String, BlurIndex> indexes, List<RowMutation> mutations)
      throws BlurException, IOException {
    long s = System.nanoTime();
    boolean waitToBeVisible = false;
    for (int i = 0; i < mutations.size(); i++) {
      RowMutation mutation = mutations.get(i);
      if (mutation.waitToBeVisible) {
        waitToBeVisible = true;
      }
      BlurIndex blurIndex = indexes.get(shard);
      if (blurIndex == null) {
        throw new BException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.");
      }

      boolean waitVisiblity = false;
      if (i + 1 == mutations.size()) {
        waitVisiblity = waitToBeVisible;
      }
      RowMutationType type = mutation.rowMutationType;
      switch (type) {
      case REPLACE_ROW:
        Row row = MutationHelper.getRowFromMutations(mutation.rowId, mutation.recordMutations);
        blurIndex.replaceRow(waitVisiblity, mutation.wal, updateMetrics(row));
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
      throw new BException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.");
    }

    RowMutationType type = mutation.rowMutationType;
    switch (type) {
    case REPLACE_ROW:
      Row row = MutationHelper.getRowFromMutations(mutation.rowId, mutation.recordMutations);
      blurIndex.replaceRow(mutation.waitToBeVisible, mutation.wal, updateMetrics(row));
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

  private Row updateMetrics(Row row) {
    _writeRowMeter.mark();
    List<Record> records = row.getRecords();
    if (records != null) {
      _writeRecordsMeter.mark(records.size());
    }
    return row;
  }

  private void doUpdateRowMutation(RowMutation mutation, BlurIndex blurIndex) throws BlurException, IOException {
    FetchResult fetchResult = new FetchResult();
    Selector selector = new Selector();
    selector.setRowId(mutation.rowId);
    fetchRow(mutation.table, selector, fetchResult);
    Row existingRow;
    if (fetchResult.exists) {
      // We will examine the contents of the existing row and add records
      // onto a new replacement row based on the mutation we have been given.
      existingRow = fetchResult.rowResult.row;
    } else {
      // The row does not exist, create empty new row.
      existingRow = new Row().setId(mutation.getRowId());
      existingRow.records = new ArrayList<Record>();
    }
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
      case REPLACE_ENTIRE_RECORD:
      case REPLACE_COLUMNS:
        // If record do not exist, create new record in Row
        newRow.addToRecords(recordMutation.record);
        break;
      default:
        throw new RuntimeException("Unsupported record mutation type [" + type + "]");
      }
    }

    // Finally, replace the existing row with the new row we have built.
    blurIndex.replaceRow(mutation.waitToBeVisible, mutation.wal, updateMetrics(newRow));

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

  private int getNumberOfShards(String table) {
    return _indexServer.getShardCount(table);
  }

  static class SimpleQueryParallelCall implements ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> {

    private final String _table;
    private final QueryStatus _status;
    private final IndexServer _indexServer;
    private final Query _query;
    private final Selector _selector;
    private final AtomicBoolean _running;
    private final Meter _queriesInternalMeter;
    private final ShardServerContext _shardServerContext;
    private final boolean _runSlow;
    private final int _fetchCount;
    private final int _maxHeapPerRowFetch;

    public SimpleQueryParallelCall(AtomicBoolean running, String table, QueryStatus status, IndexServer indexServer,
        Query query, Selector selector, Meter queriesInternalMeter, ShardServerContext shardServerContext,
        boolean runSlow, int fetchCount, int maxHeapPerRowFetch) {
      _running = running;
      _table = table;
      _status = status;
      _indexServer = indexServer;
      _query = query;
      _selector = selector;
      _queriesInternalMeter = queriesInternalMeter;
      _shardServerContext = shardServerContext;
      _runSlow = runSlow;
      _fetchCount = fetchCount;
      _maxHeapPerRowFetch = maxHeapPerRowFetch;
    }

    @Override
    public BlurResultIterable call(Entry<String, BlurIndex> entry) throws Exception {
      String shard = entry.getKey();
      _status.attachThread(shard);
      BlurIndex index = entry.getValue();
      IndexSearcherClosable searcher = index.getIndexReader();
      try {
        IndexReader indexReader = searcher.getIndexReader();
        if (indexReader instanceof ExitableReader) {
          ExitableReader er = (ExitableReader) indexReader;
          er.setRunning(_running);
        } else {
          throw new IOException("IndexReader is not ExitableReader");
        }
        if (_shardServerContext != null) {
          _shardServerContext.setIndexSearcherClosable(_table, shard, searcher);
        }
        searcher.setSimilarity(_indexServer.getSimilarity(_table));
        Query rewrite;
        try {
          rewrite = searcher.rewrite((Query) _query.clone());
        } catch (ExitingReaderException e) {
          LOG.info("Query [{0}] has been cancelled during the rewrite phase.", _query);
          throw e;
        }

        // BlurResultIterableSearcher will close searcher, if shard server
        // context is null.
        return new BlurResultIterableSearcher(_running, rewrite, _table, shard, searcher, _selector,
            _shardServerContext == null, _runSlow, _fetchCount, _maxHeapPerRowFetch);
      } catch (BlurException e) {
        switch (_status.getQueryStatus().getState()) {
        case INTERRUPTED:
          e.setErrorType(ErrorType.QUERY_CANCEL);
          throw e;
        case BACK_PRESSURE_INTERRUPTED:
          e.setErrorType(ErrorType.BACK_PRESSURE);
          throw e;
        default:
          e.setErrorType(ErrorType.UNKNOWN);
          throw e;
        }
      } finally {
        _queriesInternalMeter.mark();
        _status.deattachThread(shard);
      }
    }
  }

  public void setThreadCount(int threadCount) {
    this._threadCount = threadCount;
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

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }

  public void setFetchCount(int fetchCount) {
    _fetchCount = fetchCount;
  }

  public void setMaxHeapPerRowFetch(int maxHeapPerRowFetch) {
    _maxHeapPerRowFetch = maxHeapPerRowFetch;
  }

}
