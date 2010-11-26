package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.utils.RowSuperDocumentUtil.createSuperDocument;
import static com.nearinfinity.blur.utils.RowSuperDocumentUtil.getRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.search.BlurSearcher;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableSearcher;
import com.nearinfinity.blur.manager.hits.MergerHitsIterable;
import com.nearinfinity.blur.manager.status.SearchStatus;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.PrimeDocCache;
import com.nearinfinity.blur.utils.TermDocIterable;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class IndexManager {

    private static final Version LUCENE_VERSION = Version.LUCENE_30;
    private static final Log LOG = LogFactory.getLog(IndexManager.class);

    private IndexServer indexServer;
    private ExecutorService executor;
    private Collection<SearchStatus> currentSearchStatusCollection = Collections.synchronizedSet(new HashSet<SearchStatus>());
    private Timer searchStatusCleanupTimer;

    public IndexManager() {
        // do nothing
    }

    public void init() {
        executor = Executors.newCachedThreadPool();
        searchStatusCleanupTimer = new Timer("Search-Status-Cleanup",true);
        searchStatusCleanupTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                cleanupFinishedSearchStatuses();
            }
        }, TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(1));
        
    }

    public static void replace(IndexWriter indexWriter, Row row) throws IOException {
        replace(indexWriter, createSuperDocument(row));
    }

    public static void replace(IndexWriter indexWriter, SuperDocument document) throws IOException {
        synchronized (indexWriter) {
            indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
            if (!replaceInternal(indexWriter, document)) {
                indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
                if (!replaceInternal(indexWriter, document)) {
                    throw new IOException("SuperDocument too large, try increasing ram buffer size.");
                }
            }
        }
    }

    public void close() throws InterruptedException {
        executor.shutdownNow();
        indexServer.close();
    }

    public void replaceRow(String table, Row row) throws BlurException, MissingShardException {
        throw new RuntimeException("not implemented");
    }

    public void removeRow(String table, String id) throws BlurException {
        throw new RuntimeException("not implemented");
    }

    public void fetchRow(String table, Selector selector, FetchResult fetchResult) throws BlurException,
            MissingShardException {
        IndexReader reader;
        try {
            reader = indexServer.getIndexReader(table, selector);
        } catch (MissingShardException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get the correct index reader for selector [" + selector + "].", e);
            throw new BlurException(e.getMessage());
        }
        try {
            fetchRow(reader, table, selector, fetchResult);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to fetch row.", e);
            throw new BlurException(e.getMessage());
        }
    }

    public HitsIterable search(final String table, SearchQuery searchQuery) throws Exception {
        final SearchStatus status = new SearchStatus(table,searchQuery).attachThread();
        addStatus(status);
        try {
            Map<String, IndexReader> indexReaders;
            try {
                indexReaders = indexServer.getIndexReaders(table);
            } catch (IOException e) {
                LOG.error("Unknown error while trying to fetch index readers.", e);
                throw new BlurException(e.getMessage());
            }
            Filter preFilter = parseFilter(table, searchQuery.preSuperFilter, false, searchQuery.type);
            Filter postFilter = parseFilter(table, searchQuery.postSuperFilter, true, searchQuery.type);
            final Query userQuery = parseQuery(searchQuery.queryStr, searchQuery.superQueryOn, 
                    indexServer.getAnalyzer(table), postFilter, preFilter, searchQuery.type);
            return ForkJoin.execute(executor, indexReaders.entrySet(),
                new ParallelCall<Entry<String, IndexReader>, HitsIterable>() {
                    @Override
                    public HitsIterable call(Entry<String, IndexReader> entry) throws Exception {
                        status.attachThread();
                        try {
                            IndexReader reader = entry.getValue();
                            String shard = entry.getKey();
                            BlurSearcher searcher = new BlurSearcher(reader, 
                                    PrimeDocCache.getTableCache().getShardCache(table).
                                    getIndexReaderCache(shard));
                            searcher.setSimilarity(indexServer.getSimilarity());
                            return new HitsIterableSearcher((Query) userQuery.clone(), table, shard, searcher);
                        } finally {
                            status.deattachThread();
                        }
                    }
                }).merge(new MergerHitsIterable(searchQuery.minimumNumberOfHits, searchQuery.maxQueryTime));
        } finally {
            status.deattachThread();
            removeStatus(status);
        }
    }

    public void cancelSearch(long userUuid) {
        for (SearchStatus status : currentSearchStatusCollection) {
            if (status.getUserUuid() == userUuid) {
                status.cancelSearch();
            }
        }
    }

    public List<SearchQueryStatus> currentSearches(String table) {
        List<SearchQueryStatus> result = new ArrayList<SearchQueryStatus>();
        for (SearchStatus status : currentSearchStatusCollection) {
            if (status.getTable().equals(table)) {
                result.add(status.getSearchQueryStatus());
            }
        }
        return result;
    }

    private Filter parseFilter(String table, String filter, boolean superQueryOn, ScoreType scoreType)
            throws ParseException, BlurException {
        if (filter == null) {
            return null;
        }
        return new QueryWrapperFilter(new SuperParser(LUCENE_VERSION, indexServer.getAnalyzer(table), superQueryOn,
                null, scoreType).parse(filter));
    }

    private Query parseQuery(String query, boolean superQueryOn, Analyzer analyzer, Filter postFilter,
            Filter preFilter, ScoreType scoreType) throws ParseException {
        Query result = new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, preFilter, scoreType).parse(query);
        if (postFilter == null) {
            return result;
        }
        return new FilteredQuery(result, postFilter);
    }

    private void fetchRow(IndexReader reader, String table, Selector selector, FetchResult fetchResult)
            throws CorruptIndexException, IOException {
        fetchResult.table = table;
        String locationId = selector.locationId;
        int lastSlash = locationId.lastIndexOf('/');
        int docId = Integer.parseInt(locationId.substring(lastSlash + 1));
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
                fetchResult.record = getColumns(document);
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
                TermDocs termDocs = reader.termDocs(new Term(SuperDocument.ID, rowId));
                fetchResult.row = getRow(new TermDocIterable(termDocs, reader));
                return;
            }
        }
    }

    private String getRowId(IndexReader reader, int docId) throws CorruptIndexException, IOException {
        Document document = reader.document(docId, new FieldSelector() {
            private static final long serialVersionUID = 4912420100148752051L;

            @Override
            public FieldSelectorResult accept(String fieldName) {
                if (SuperDocument.ID.equals(fieldName)) {
                    return FieldSelectorResult.LOAD_AND_BREAK;
                }
                return FieldSelectorResult.NO_LOAD;
            }
        });
        return document.get(SuperDocument.ID);
    }

    private Set<Column> getColumns(Document document) {
        Map<String, Column> columns = new HashMap<String, Column>();
        List<Fieldable> fields = document.getFields();
        String columnFamily = null;
        for (Fieldable field : fields) {
            String name = field.name();
            if (columnFamily == null) {
                columnFamily = getColumnFamily(name);
            }
            String value = field.stringValue();
            Column column = columns.get(name);
            if (column == null) {
                column = new Column();
                column.setName(getColumnName(name));
                columns.put(name, column);
            }
            column.addToValues(value);
        }
        Set<Column> cols = new HashSet<Column>(columns.values());
        if (columnFamily != null) {
            Column column = new Column().setName("_columnFamily_");
            column.addToValues(columnFamily);
            cols.add(column);
        }
        return cols;
    }

    private String getColumnName(String fieldName) {
        return fieldName.substring(fieldName.lastIndexOf('.') + 1);
    }

    private String getColumnFamily(String fieldName) {
        return fieldName.substring(0, fieldName.lastIndexOf('.'));
    }

    private FieldSelector getFieldSelector(final Selector selector) {
        return new FieldSelector() {
            private static final long serialVersionUID = 4089164344758433000L;

            @Override
            public FieldSelectorResult accept(String fieldName) {
                if (SuperDocument.ID.equals(fieldName)) {
                    return FieldSelectorResult.LOAD;
                }
                if (SuperDocument.SUPER_KEY.equals(fieldName)) {
                    return FieldSelectorResult.LOAD;
                }
                if (SuperDocument.PRIME_DOC.equals(fieldName)) {
                    return FieldSelectorResult.NO_LOAD;
                }
                if (selector.columnFamilies == null && selector.columns == null) {
                    return FieldSelectorResult.LOAD;
                }
                String columnFamily = getColumnFamily(fieldName);
                if (selector.columnFamilies != null && selector.columnFamilies.contains(columnFamily)) {
                    return FieldSelectorResult.LOAD;
                }
                String columnName = getColumnName(fieldName);
                if (selector.columns != null) {
                    Set<String> columns = selector.columns.get(columnFamily);
                    if (columns != null && columns.contains(columnName)) {
                        return FieldSelectorResult.LOAD;
                    }
                }
                return FieldSelectorResult.NO_LOAD;
            }
        };
    }

    private static boolean replaceInternal(IndexWriter indexWriter, SuperDocument document) throws IOException {
        long oldRamSize = indexWriter.ramSizeInBytes();
        for (Document doc : document.getAllDocumentsForIndexing()) {
            long newRamSize = indexWriter.ramSizeInBytes();
            if (newRamSize < oldRamSize) {
                LOG.info("Flush occur during writing of super document, start over.");
                return false;
            }
            oldRamSize = newRamSize;
            indexWriter.addDocument(doc);
        }
        return true;
    }

    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }
    
    private void removeStatus(SearchStatus status) {
        status.setFinished(true);
    }

    private void addStatus(SearchStatus status) {
        currentSearchStatusCollection.add(status);
    }
    
    private void cleanupFinishedSearchStatuses() {
        Collection<SearchStatus> remove = new HashSet<SearchStatus>();
        for (SearchStatus status : currentSearchStatusCollection) {
            if (status.isValidForCleanUp()) {
                remove.add(status);
            }
        }
        currentSearchStatusCollection.removeAll(remove);
    }
}
