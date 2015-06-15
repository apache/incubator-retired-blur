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
import static org.apache.blur.utils.BlurConstants.FAMILY;
import static org.apache.blur.utils.BlurConstants.PRIME_DOC;
import static org.apache.blur.utils.BlurConstants.RECORD_ID;
import static org.apache.blur.utils.BlurConstants.ROW_ID;
import static org.apache.blur.utils.RowDocumentUtil.getRecord;
import static org.apache.blur.utils.RowDocumentUtil.getRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.concurrent.Executors;
import org.apache.blur.index.AtomicReaderUtil;
import org.apache.blur.index.ExitableReader;
import org.apache.blur.index.ExitableReader.ExitingReaderException;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.DeepPagingCache;
import org.apache.blur.lucene.search.FacetExecutor;
import org.apache.blur.lucene.search.FacetQuery;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.StopExecutionCollector.StopExecutionCollectorException;
import org.apache.blur.lucene.security.index.SecureDirectoryReader;
import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.manager.results.BlurResultIterable;
import org.apache.blur.manager.results.BlurResultIterableSearcher;
import org.apache.blur.manager.results.MergerBlurResultIterable;
import org.apache.blur.manager.status.QueryStatus;
import org.apache.blur.manager.status.QueryStatusManager;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.MutatableAction;
import org.apache.blur.memory.MemoryAllocationWatcher;
import org.apache.blur.memory.Watcher;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.ShardServerContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.MutationHelper;
import org.apache.blur.thrift.UserConverter;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.Facet;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.HighlightOptions;
import org.apache.blur.thrift.generated.QueryState;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.BlurExecutorCompletionService.Cancel;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ForkJoin;
import org.apache.blur.utils.ForkJoin.Merger;
import org.apache.blur.utils.ForkJoin.ParallelCall;
import org.apache.blur.utils.HighlightHelper;
import org.apache.blur.utils.ResetableDocumentStoredFieldVisitor;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.BaseCompositeReaderUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class IndexManager {

  public static final String NOT_FOUND = "NOT_FOUND";
  private static final Log LOG = LogFactory.getLog(IndexManager.class);

  private static final Meter _readRecordsMeter;
  private static final Meter _readRowMeter;

  static {
    MetricName metricName1 = new MetricName(ORG_APACHE_BLUR, BLUR, "Read Records/s");
    MetricName metricName2 = new MetricName(ORG_APACHE_BLUR, BLUR, "Read Row/s");
    _readRecordsMeter = Metrics.newMeter(metricName1, "Records/s", TimeUnit.SECONDS);
    _readRowMeter = Metrics.newMeter(metricName2, "Row/s", TimeUnit.SECONDS);
  }

  private final Meter _queriesExternalMeter;
  private final Meter _queriesInternalMeter;

  private final IndexServer _indexServer;
  private final ClusterStatus _clusterStatus;
  private final ExecutorService _executor;
  private final ExecutorService _facetExecutor;
  private final ExecutorService _mutateExecutor;

  private final QueryStatusManager _statusManager = new QueryStatusManager();
  private final AtomicBoolean _closed = new AtomicBoolean(false);
  private final BlurPartitioner _blurPartitioner = new BlurPartitioner();
  private final BlurFilterCache _filterCache;
  private final long _defaultParallelCallTimeout = TimeUnit.MINUTES.toMillis(1);

  private final Timer _fetchTimer;
  private final int _fetchCount;
  private final int _maxHeapPerRowFetch;

  private final int _threadCount;
  private final int _mutateThreadCount;
  private final DeepPagingCache _deepPagingCache;
  private final MemoryAllocationWatcher _memoryAllocationWatcher;

  public static AtomicBoolean DEBUG_RUN_SLOW = new AtomicBoolean(false);

  public IndexManager(IndexServer indexServer, ClusterStatus clusterStatus, BlurFilterCache filterCache,
      int maxHeapPerRowFetch, int fetchCount, int threadCount, int mutateThreadCount, long statusCleanupTimerDelay,
      int facetThreadCount, DeepPagingCache deepPagingCache, MemoryAllocationWatcher memoryAllocationWatcher) {
    _memoryAllocationWatcher = memoryAllocationWatcher;
    _deepPagingCache = deepPagingCache;
    _indexServer = indexServer;
    _clusterStatus = clusterStatus;
    _filterCache = filterCache;

    MetricName metricName1 = new MetricName(ORG_APACHE_BLUR, BLUR, "External Queries/s");
    MetricName metricName2 = new MetricName(ORG_APACHE_BLUR, BLUR, "Internal Queries/s");
    MetricName metricName3 = new MetricName(ORG_APACHE_BLUR, BLUR, "Fetch Timer");

    _queriesExternalMeter = Metrics.newMeter(metricName1, "External Queries/s", TimeUnit.SECONDS);
    _queriesInternalMeter = Metrics.newMeter(metricName2, "Internal Queries/s", TimeUnit.SECONDS);
    _fetchTimer = Metrics.newTimer(metricName3, TimeUnit.MICROSECONDS, TimeUnit.SECONDS);

    if (threadCount == 0) {
      throw new RuntimeException("Thread Count cannot be 0.");
    }
    _threadCount = threadCount;
    if (mutateThreadCount == 0) {
      throw new RuntimeException("Mutate Thread Count cannot be 0.");
    }
    _mutateThreadCount = mutateThreadCount;
    _fetchCount = fetchCount;
    _maxHeapPerRowFetch = maxHeapPerRowFetch;

    _executor = Executors.newThreadPool("index-manager", _threadCount);
    _mutateExecutor = Executors.newThreadPool("index-manager-mutate", _mutateThreadCount);
    if (facetThreadCount < 1) {
      _facetExecutor = null;
    } else {
      _facetExecutor = Executors.newThreadPool(new SynchronousQueue<Runnable>(), "facet-execution", facetThreadCount);
    }

    _statusManager.setStatusCleanupTimerDelay(statusCleanupTimerDelay);
    _statusManager.init();
    LOG.info("Init Complete");

  }

  public synchronized void close() {
    if (!_closed.get()) {
      _closed.set(true);
      _statusManager.close();
      _executor.shutdownNow();
      _mutateExecutor.shutdownNow();
      if (_facetExecutor != null) {
        _facetExecutor.shutdownNow();
      }
      try {
        _indexServer.close();
      } catch (IOException e) {
        LOG.error("Unknown error while trying to close the index server", e);
      }
    }
  }

  public List<FetchResult> fetchRowBatch(final String table, List<Selector> selectors) throws BlurException {
    List<Future<FetchResult>> futures = new ArrayList<Future<FetchResult>>();
    for (Selector s : selectors) {
      final Selector selector = s;
      futures.add(_executor.submit(new Callable<FetchResult>() {
        @Override
        public FetchResult call() throws Exception {
          FetchResult fetchResult = new FetchResult();
          fetchRow(table, selector, fetchResult);
          return fetchResult;
        }
      }));
    }
    List<FetchResult> results = new ArrayList<FetchResult>();
    for (Future<FetchResult> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        throw new BException("Unkown error while fetching batch table [{0}] selectors [{1}].", e, table, selectors);
      } catch (ExecutionException e) {
        throw new BException("Unkown error while fetching batch table [{0}] selectors [{1}].", e.getCause(), table,
            selectors);
      }
    }
    return results;
  }

  public void fetchRow(String table, Selector selector, FetchResult fetchResult) throws BlurException {
    validSelector(selector);
    TableContext tableContext = getTableContext(table);
    ReadInterceptor interceptor = tableContext.getReadInterceptor();
    Filter filter = interceptor.getFilter();
    BlurIndex index = null;
    String shard = null;
    Tracer trace = Trace.trace("manager fetch", Trace.param("table", table));
    IndexSearcherCloseable searcher = null;
    try {
      if (selector.getLocationId() == null) {
        // Not looking up by location id so we should resetSearchers.
        ShardServerContext.resetSearchers();
        shard = MutationHelper.getShardName(table, selector.rowId, getNumberOfShards(table), _blurPartitioner);
        index = getBlurIndex(table, shard);
        searcher = index.getIndexSearcher();
        populateSelector(searcher, shard, table, selector);
      }
      String locationId = selector.getLocationId();
      if (locationId.equals(NOT_FOUND)) {
        fetchResult.setDeleted(false);
        fetchResult.setExists(false);
        return;
      }
      if (shard == null) {
        shard = getShard(locationId);
      }
      if (index == null) {
        index = getBlurIndex(table, shard);
      }
    } catch (BlurException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Unknown error while trying to get the correct index reader for selector [{0}].", e, selector);
      throw new BException(e.getMessage(), e);
    }
    TimerContext timerContext = _fetchTimer.time();
    boolean usedCache = true;
    try {
      ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
      if (shardServerContext != null) {
        searcher = shardServerContext.getIndexSearcherClosable(table, shard);
      }
      if (searcher == null) {
        // Was not pulled from cache, get a fresh one from the index.
        searcher = index.getIndexSearcher();
        usedCache = false;
      }
      FieldManager fieldManager = tableContext.getFieldManager();

      Query highlightQuery = getHighlightQuery(selector, table, fieldManager);

      fetchRow(searcher.getIndexReader(), table, shard, selector, fetchResult, highlightQuery, fieldManager,
          _maxHeapPerRowFetch, tableContext, filter);
    } catch (Exception e) {
      LOG.error("Unknown error while trying to fetch row.", e);
      throw new BException(e.getMessage(), e);
    } finally {
      trace.done();
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

  private BlurIndex getBlurIndex(String table, String shard) throws BException, IOException {
    Map<String, BlurIndex> blurIndexes = _indexServer.getIndexes(table);
    if (blurIndexes == null) {
      LOG.error("Table [{0}] not found", table);
      // @TODO probably should make a enum for not found on this server so the
      // controller knows to try another server.
      throw new BException("Table [" + table + "] not found");
    }
    BlurIndex index = blurIndexes.get(shard);
    if (index == null) {
      LOG.error("Shard [{0}] not found in table [{1}]", shard, table);
      // @TODO probably should make a enum for not found on this server so the
      // controller knows to try another server.
      throw new BException("Shard [" + shard + "] not found in table [" + table + "]");
    }
    return index;
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

  public static void populateSelector(IndexSearcherCloseable searcher, String shardName, String table, Selector selector)
      throws IOException {
    Tracer trace = Trace.trace("populate selector");
    String rowId = selector.rowId;
    String recordId = selector.recordId;
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
      trace.done();
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
    User user = UserContext.getUser();
    final QueryStatus status = _statusManager.newQueryStatus(table, blurQuery, _threadCount, running,
        UserConverter.toThriftUser(user));
    _queriesExternalMeter.mark();
    try {
      Map<String, BlurIndex> blurIndexes;
      try {
        blurIndexes = _indexServer.getIndexes(table);
      } catch (IOException e) {
        LOG.error("Unknown error while trying to fetch index readers.", e);
        throw new BException(e.getMessage(), e);
      }
      String rowId = blurQuery.getRowId();
      if (rowId != null) {
        // reduce the index selection down to the only one that would contain
        // the row.
        Map<String, BlurIndex> map = new HashMap<String, BlurIndex>();
        String shard = MutationHelper.getShardName(table, rowId, getNumberOfShards(table), _blurPartitioner);
        BlurIndex index = getBlurIndex(table, shard);
        map.put(shard, index);
        blurIndexes = map;
      }
      Tracer trace = Trace.trace("query setup", Trace.param("table", table));
      ShardServerContext shardServerContext = ShardServerContext.getShardServerContext();
      ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> call;
      TableContext context = getTableContext(table);
      FieldManager fieldManager = context.getFieldManager();
      org.apache.blur.thrift.generated.Query simpleQuery = blurQuery.query;
      ReadInterceptor interceptor = context.getReadInterceptor();
      Filter readFilter = interceptor.getFilter();
      if (rowId != null) {
        if (simpleQuery.recordFilter == null) {
          simpleQuery.recordFilter = "+" + BlurConstants.ROW_ID + ":" + rowId;
        } else {
          simpleQuery.recordFilter = "+" + BlurConstants.ROW_ID + ":" + rowId + " +(" + simpleQuery.recordFilter + ")";
        }
      }
      Filter recordFilterForSearch = QueryParserUtil.parseFilter(table, simpleQuery.recordFilter, false, fieldManager,
          _filterCache, context);
      Filter rowFilterForSearch = QueryParserUtil.parseFilter(table, simpleQuery.rowFilter, true, fieldManager,
          _filterCache, context);
      Filter docFilter;
      if (recordFilterForSearch == null && readFilter != null) {
        docFilter = readFilter;
      } else if (recordFilterForSearch != null && readFilter == null) {
        docFilter = recordFilterForSearch;
      } else if (recordFilterForSearch != null && readFilter != null) {
        // @TODO dangerous call because of the bitsets that booleanfilter
        // creates.
        BooleanFilter booleanFilter = new BooleanFilter();
        booleanFilter.add(recordFilterForSearch, Occur.MUST);
        booleanFilter.add(readFilter, Occur.MUST);
        docFilter = booleanFilter;
      } else {
        docFilter = null;
      }
      Query userQuery = QueryParserUtil.parseQuery(simpleQuery.query, simpleQuery.rowQuery, fieldManager,
          rowFilterForSearch, docFilter, getScoreType(simpleQuery.scoreType), context);

      Query facetedQuery;
      FacetExecutor executor = null;
      if (blurQuery.facets != null) {
        long[] facetMinimums = getFacetMinimums(blurQuery.facets);
        executor = new FacetExecutor(blurQuery.facets.size(), facetMinimums, facetedCounts, running);
        facetedQuery = new FacetQuery(userQuery, getFacetQueries(blurQuery, fieldManager, context, rowFilterForSearch,
            recordFilterForSearch), executor);
      } else {
        facetedQuery = userQuery;
      }

      Sort sort = getSort(blurQuery, fieldManager);
      call = new SimpleQueryParallelCall(running, table, status, facetedQuery, blurQuery.selector,
          _queriesInternalMeter, shardServerContext, runSlow, _fetchCount, _maxHeapPerRowFetch,
          context.getSimilarity(), context, sort, _deepPagingCache, _memoryAllocationWatcher);
      trace.done();
      MergerBlurResultIterable merger = new MergerBlurResultIterable(blurQuery);
      BlurResultIterable merge = ForkJoin.execute(_executor, blurIndexes.entrySet(), call, new Cancel() {
        @Override
        public void cancel() {
          running.set(false);
        }
      }).merge(merger);

      if (executor != null) {
        executor.processFacets(_facetExecutor);
      }
      return fetchDataIfNeeded(merge, table, blurQuery.getSelector());
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

  private Sort getSort(BlurQuery blurQuery, FieldManager fieldManager) throws IOException, BlurException {
    List<org.apache.blur.thrift.generated.SortField> sortFields = blurQuery.getSortFields();
    if (sortFields == null || sortFields.isEmpty()) {
      return null;
    }
    SortField[] fields = new SortField[sortFields.size()];
    int i = 0;
    for (org.apache.blur.thrift.generated.SortField sortField : sortFields) {
      if (sortField == null) {
        throw new BException("Sortfields [{0}] can not contain a null.", sortFields);
      }
      String fieldName = getFieldName(sortField);
      SortField field = fieldManager.getSortField(fieldName, sortField.reverse);
      fields[i++] = field;
    }
    return new Sort(fields);
  }

  private String getFieldName(org.apache.blur.thrift.generated.SortField sortField) throws BlurException {
    String family = sortField.getFamily();
    if (family == null) {
      family = BlurConstants.DEFAULT_FAMILY;
    }
    String column = sortField.getColumn();
    if (column == null) {
      throw new BException("Column in sortfield [{0}] can not be null.", sortField);
    }
    return family + "." + column;
  }

  private BlurResultIterable fetchDataIfNeeded(final BlurResultIterable iterable, final String table,
      final Selector selector) {
    if (selector == null) {
      return iterable;
    }
    return new BlurResultIterable() {

      @Override
      public BlurIterator<BlurResult, BlurException> iterator() throws BlurException {
        final BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
        return new BlurIterator<BlurResult, BlurException>() {

          @Override
          public BlurResult next() throws BlurException {
            BlurResult result = iterator.next();
            String locationId = result.getLocationId();
            FetchResult fetchResult = new FetchResult();
            Selector s = new Selector(selector);
            s.setLocationId(locationId);
            fetchRow(table, s, fetchResult);
            result.setFetchResult(fetchResult);
            return result;
          }

          @Override
          public boolean hasNext() throws BlurException {
            return iterator.hasNext();
          }

          @Override
          public long getPosition() throws BlurException {
            return iterator.getPosition();
          }
        };
      }

      @Override
      public void close() throws IOException {
        iterable.close();
      }

      @Override
      public void skipTo(long skipTo) {
        iterable.skipTo(skipTo);
      }

      @Override
      public long getTotalResults() {
        return iterable.getTotalResults();
      }

      @Override
      public Map<String, Long> getShardInfo() {
        return iterable.getShardInfo();
      }
    };
  }

  private long[] getFacetMinimums(List<Facet> facets) {
    long[] mins = new long[facets.size()];
    boolean smallerThanMaxLong = false;
    for (int i = 0; i < facets.size(); i++) {
      Facet facet = facets.get(i);
      if (facet != null) {
        long minimumNumberOfBlurResults = facet.getMinimumNumberOfBlurResults();
        mins[i] = minimumNumberOfBlurResults;
        if (minimumNumberOfBlurResults < Long.MAX_VALUE) {
          smallerThanMaxLong = true;
        }
      }
    }
    if (smallerThanMaxLong) {
      return mins;
    }
    return null;
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
      FetchResult fetchResult, Query highlightQuery, int maxHeap, TableContext tableContext, Filter filter)
      throws CorruptIndexException, IOException {
    fetchRow(reader, table, shard, selector, fetchResult, highlightQuery, null, maxHeap, tableContext, filter);
  }

  public static void fetchRow(IndexReader reader, String table, String shard, Selector selector,
      FetchResult fetchResult, Query highlightQuery, FieldManager fieldManager, int maxHeap, TableContext tableContext,
      Filter filter) throws CorruptIndexException, IOException {
    try {
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

      Tracer t1 = Trace.trace("fetchRow - live docs");
      Bits liveDocs = MultiFields.getLiveDocs(reader);
      t1.done();
      ResetableDocumentStoredFieldVisitor fieldVisitor = getFieldSelector(selector);
      if (selector.isRecordOnly()) {
        // select only the row for the given data or location id.
        if (isFiltered(docId, reader, filter)) {
          fetchResult.exists = false;
          fetchResult.deleted = false;
          return;
        } else if (liveDocs != null && !liveDocs.get(docId)) {
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
              document = HighlightHelper.highlight(docId, document, highlightQuery, fieldManager, reader, preTag,
                  postTag);
            } catch (InvalidTokenOffsetsException e) {
              LOG.error("Unknown error while tring to highlight", e);
            }
          }
          fieldVisitor.reset();
          fetchResult.recordResult = getRecord(document);
          return;
        }
      } else {
        Tracer trace = Trace.trace("fetchRow - Row read");
        try {
          if (liveDocs != null && !liveDocs.get(docId)) {
            fetchResult.exists = false;
            fetchResult.deleted = true;
            return;
          } else {
            fetchResult.exists = true;
            fetchResult.deleted = false;
            if (returnIdsOnly) {
              String rowId = selector.getRowId();
              if (rowId == null) {
                rowId = getRowId(reader, docId);
              }
              fetchResult.rowResult = new FetchRowResult();
              fetchResult.rowResult.row = new Row(rowId, null);
            } else {
              List<Document> docs;
              AtomicBoolean moreDocsToFetch = new AtomicBoolean(false);
              AtomicInteger totalRecords = new AtomicInteger();
              BlurHighlighter highlighter = new BlurHighlighter(highlightQuery, fieldManager, selector);
              Tracer docTrace = Trace.trace("fetchRow - Document read");
              docs = BlurUtil.fetchDocuments(reader, fieldVisitor, selector, maxHeap, table + "/" + shard,
                  tableContext.getDefaultPrimeDocTerm(), filter, moreDocsToFetch, totalRecords, highlighter);
              docTrace.done();
              Tracer rowTrace = Trace.trace("fetchRow - Row create");
              Row row = getRow(docs);
              if (row == null) {
                String rowId = selector.getRowId();
                if (rowId == null) {
                  rowId = getRowId(reader, docId);
                }
                row = new Row(rowId, null);
              }
              fetchResult.rowResult = new FetchRowResult(row, selector.getStartRecord(),
                  selector.getMaxRecordsToFetch(), moreDocsToFetch.get(), totalRecords.get());
              rowTrace.done();
            }
            return;
          }
        } finally {
          trace.done();
        }
      }
    } finally {
      if (fetchResult.rowResult != null) {
        if (fetchResult.rowResult.row != null && fetchResult.rowResult.row.records != null) {
          _readRecordsMeter.mark(fetchResult.rowResult.row.records.size());
        }
        _readRowMeter.mark();
      } else if (fetchResult.recordResult != null) {
        _readRecordsMeter.mark();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean isFiltered(int notAdjustedDocId, IndexReader reader, Filter filter) throws IOException {
    if (filter == null) {
      return false;
    }
    if (reader instanceof BaseCompositeReader) {
      BaseCompositeReader<IndexReader> indexReader = (BaseCompositeReader<IndexReader>) reader;
      List<? extends IndexReader> sequentialSubReaders = BaseCompositeReaderUtil.getSequentialSubReaders(indexReader);
      int readerIndex = BaseCompositeReaderUtil.readerIndex(indexReader, notAdjustedDocId);
      int readerBase = BaseCompositeReaderUtil.readerBase(indexReader, readerIndex);
      int docId = notAdjustedDocId - readerBase;
      IndexReader orgReader = sequentialSubReaders.get(readerIndex);
      SegmentReader sReader = AtomicReaderUtil.getSegmentReader(orgReader);
      if (sReader != null) {
        SegmentReader segmentReader = (SegmentReader) sReader;
        DocIdSet docIdSet = filter.getDocIdSet(segmentReader.getContext(), segmentReader.getLiveDocs());
        DocIdSetIterator iterator = docIdSet.iterator();
        if (iterator == null) {
          return true;
        }
        if (iterator.advance(docId) == docId) {
          return false;
        }
        return true;
      }
      throw new RuntimeException("Reader has to be a SegmentReader [" + orgReader + "]");
    } else {
      throw new RuntimeException("Reader has to be a BaseCompositeReader [" + reader + "]");
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
        IndexSearcherCloseable searcher = index.getIndexSearcher();
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
            IndexSearcherCloseable searcher = index.getIndexSearcher();
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
        SortedSet<String> terms = new TreeSet<String>();
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

    if (termsAll == null) {
      return terms;
    }

    TermsEnum termEnum = termsAll.iterator(null);
    SeekStatus status = termEnum.seekCeil(term.bytes());

    if (status == SeekStatus.END) {
      return terms;
    }

    BytesRef currentTermText = termEnum.term();
    do {
      terms.add(currentTermText.utf8ToString());
      if (terms.size() >= size) {
        return terms;
      }
    } while ((currentTermText = termEnum.next()) != null);
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

  public void mutate(final RowMutation mutation) throws BlurException, IOException {
    long s = System.nanoTime();
    doMutates(Arrays.asList(mutation));
    long e = System.nanoTime();
    LOG.debug("doMutate took [{0} ms] to complete", (e - s) / 1000000.0);
  }

  public void mutate(final List<RowMutation> mutations) throws BlurException, IOException {
    long s = System.nanoTime();
    doMutates(mutations);
    long e = System.nanoTime();
    LOG.debug("doMutates took [{0} ms] to complete", (e - s) / 1000000.0);
  }

  private void doMutates(List<RowMutation> mutations) throws BlurException, IOException {
    mutations = MutatableAction.reduceMutates(mutations);
    Map<String, List<RowMutation>> map = getMutatesPerTable(mutations);
    for (Entry<String, List<RowMutation>> entry : map.entrySet()) {
      doMutates(entry.getKey(), entry.getValue());
    }
  }

  public void enqueue(List<RowMutation> mutations) throws BlurException, IOException {
    mutations = MutatableAction.reduceMutates(mutations);
    Map<String, List<RowMutation>> map = getMutatesPerTable(mutations);
    for (Entry<String, List<RowMutation>> entry : map.entrySet()) {
      doEnqueue(entry.getKey(), entry.getValue());
    }
  }

  private void doEnqueue(final String table, List<RowMutation> mutations) throws IOException, BlurException {
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
    for (Entry<String, List<RowMutation>> entry : mutationsByShard.entrySet()) {
      BlurIndex index = indexes.get(entry.getKey());
      index.enqueue(entry.getValue());
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
      futures.add(executeMutates(table, shard, indexes, value));
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

  private Future<Void> executeMutates(String table, String shard, Map<String, BlurIndex> indexes,
      List<RowMutation> mutations) throws BlurException, IOException {
    long s = System.nanoTime();
    try {
      final BlurIndex blurIndex = indexes.get(shard);
      if (blurIndex == null) {
        throw new BException("Shard [" + shard + "] in table [" + table + "] is not being served by this server.");
      }
      ShardContext shardContext = blurIndex.getShardContext();
      final MutatableAction mutatableAction = new MutatableAction(shardContext);
      mutatableAction.mutate(mutations);
      return _mutateExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          blurIndex.process(mutatableAction);
          return null;
        }
      });
    } finally {
      long e = System.nanoTime();
      LOG.debug("executeMutates took [{0} ms] to complete", (e - s) / 1000000.0);
    }
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

  private int getNumberOfShards(String table) {
    return getTableContext(table).getDescriptor().getShardCount();
  }

  static class SimpleQueryParallelCall implements ParallelCall<Entry<String, BlurIndex>, BlurResultIterable> {

    private final String _table;
    private final QueryStatus _status;
    private final Query _query;
    private final Selector _selector;
    private final AtomicBoolean _running;
    private final Meter _queriesInternalMeter;
    private final ShardServerContext _shardServerContext;
    private final boolean _runSlow;
    private final int _fetchCount;
    private final int _maxHeapPerRowFetch;
    private final Similarity _similarity;
    private final TableContext _context;
    private final Sort _sort;
    private final DeepPagingCache _deepPagingCache;
    private final MemoryAllocationWatcher _memoryAllocationWatcher;

    public SimpleQueryParallelCall(AtomicBoolean running, String table, QueryStatus status, Query query,
        Selector selector, Meter queriesInternalMeter, ShardServerContext shardServerContext, boolean runSlow,
        int fetchCount, int maxHeapPerRowFetch, Similarity similarity, TableContext context, Sort sort,
        DeepPagingCache deepPagingCache, MemoryAllocationWatcher memoryAllocationWatcher) {
      _running = running;
      _table = table;
      _status = status;
      _query = query;
      _selector = selector;
      _queriesInternalMeter = queriesInternalMeter;
      _shardServerContext = shardServerContext;
      _runSlow = runSlow;
      _fetchCount = fetchCount;
      _maxHeapPerRowFetch = maxHeapPerRowFetch;
      _similarity = similarity;
      _context = context;
      _sort = sort;
      _deepPagingCache = deepPagingCache;
      _memoryAllocationWatcher = memoryAllocationWatcher;
    }

    @Override
    public BlurResultIterable call(Entry<String, BlurIndex> entry) throws Exception {
      final String shard = entry.getKey();
      _status.attachThread(shard);
      BlurIndex index = entry.getValue();
      final IndexSearcherCloseable searcher = index.getIndexSearcher();
      Tracer trace2 = null;
      try {
        IndexReader indexReader = searcher.getIndexReader();
        if (!resetExitableReader(indexReader, _running)) {
          throw new IOException("Cannot find ExitableReader in stack.");
        }
        if (_shardServerContext != null) {
          _shardServerContext.setIndexSearcherClosable(_table, shard, searcher);
        }
        searcher.setSimilarity(_similarity);
        Tracer trace1 = Trace.trace("query rewrite", Trace.param("table", _table));
        final Query rewrite;
        try {
          rewrite = searcher.rewrite((Query) _query.clone());
        } catch (ExitingReaderException e) {
          LOG.info("Query [{0}] has been cancelled during the rewrite phase.", _query);
          throw e;
        } finally {
          trace1.done();
        }

        // BlurResultIterableSearcher will close searcher, if shard server
        // context is null.
        trace2 = Trace.trace("query initial search");
        BlurResultIterableSearcher iterableSearcher = _memoryAllocationWatcher
            .run(new Watcher<BlurResultIterableSearcher, BlurException>() {
              @Override
              public BlurResultIterableSearcher run() throws BlurException {
                return new BlurResultIterableSearcher(_running, rewrite, _table, shard, searcher, _selector,
                    _shardServerContext == null, _runSlow, _fetchCount, _maxHeapPerRowFetch, _context, _sort,
                    _deepPagingCache);
              }
            });
        return iterableSearcher;
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
        if (trace2 != null) {
          trace2.done();
        }
        _queriesInternalMeter.mark();
        _status.deattachThread(shard);
      }
    }

  }

  private static boolean resetExitableReader(IndexReader indexReader, AtomicBoolean running) {
    if (indexReader instanceof ExitableReader) {
      ExitableReader exitableReader = (ExitableReader) indexReader;
      exitableReader.setRunning(running);
      return true;
    }
    if (indexReader instanceof SecureDirectoryReader) {
      SecureDirectoryReader secureDirectoryReader = (SecureDirectoryReader) indexReader;
      DirectoryReader original = secureDirectoryReader.getOriginal();
      return resetExitableReader(original, running);
    }
    return false;
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

  public void enqueue(RowMutation mutation) throws BlurException, IOException {
    enqueue(Arrays.asList(mutation));
  }

  public void bulkMutateAdd(String table, String bulkId, RowMutation mutation) throws BlurException, IOException {
    String shard = MutationHelper.getShardName(table, mutation.rowId, getNumberOfShards(table), _blurPartitioner);
    Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
    BlurIndex blurIndex = indexes.get(shard);
    if (blurIndex == null) {
      throw new BException("Shard [{0}] for table [{1}] not found on this server.", shard, table);
    }
    blurIndex.addBulkMutate(bulkId, mutation);
  }

  public void bulkMutateFinish(Set<String> potentialTables, String bulkId, boolean apply, boolean blockUntilComplete)
      throws BlurException, IOException {
    for (String table : potentialTables) {
      Map<String, BlurIndex> indexes = _indexServer.getIndexes(table);
      for (BlurIndex index : indexes.values()) {
        index.finishBulkMutate(bulkId, apply, blockUntilComplete);
      }
    }
  }

  public void bulkMutateAddMultiple(String table, String bulkId, List<RowMutation> rowMutations) throws BlurException,
      IOException {
    for (RowMutation mutation : rowMutations) {
      bulkMutateAdd(table, bulkId, mutation);
    }
  }

}
