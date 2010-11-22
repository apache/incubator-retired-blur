package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.utils.RowSuperDocumentUtil.createSuperDocument;
import static com.nearinfinity.blur.utils.RowSuperDocumentUtil.getRow;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
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
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
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
	
	public IndexManager() {
	    //do nothing
	}
	
	public void init() {
	    executor = Executors.newCachedThreadPool();
	}
	
	public static void replace(IndexWriter indexWriter, Row row) throws IOException {
		replace(indexWriter,createSuperDocument(row));
	}
	
	public static void replace(IndexWriter indexWriter, SuperDocument document) throws IOException {
		synchronized (indexWriter) {
			indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
			if (!replaceInternal(indexWriter,document)) {
				indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
				if (!replaceInternal(indexWriter,document)) {
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
	    try {
    		IndexWriter indexWriter = indexServer.getIndexWriter(table, row.id);
    		checkIfShardIsNull(indexWriter);
    		try {
    			replace(indexWriter,row);
    		} catch (IOException e) {
    			LOG.error("Unknown error",e);
    			throw new BlurException("Unknown error [" + e.getMessage() + "]");
    		}
	    } catch (Exception e) {
	        LOG.error("Unknown error",e);
            throw new BlurException("Unknown error [" + e.getMessage() + "]");
        }
	}

	public void removeRow(String table, String id) throws BlurException {
		//@todo finish
	}

	public Row fetchRow(String table, Selector selector) throws BlurException, MissingShardException {
		try {
			IndexReader reader = indexServer.getIndexReader(table,selector.id);
			checkIfShardIsNull(reader);
			return getRow(selector, getDocs(reader,selector.id));
		} catch (Exception e) {
			LOG.error("Unknown error while trying to fetch row.",e);
			throw new BlurException(e.getMessage());
		}
	}
	
	public HitsIterable search(final String table, String query, final boolean superQueryOn,
			ScoreType type, String postSuperFilter, String preSuperFilter,
			long minimumNumberOfHits, long maxQueryTime) throws Exception {
		Map<String, IndexReader> indexReaders;
		try {
			indexReaders = indexServer.getIndexReaders(table);
		} catch (IOException e) {
			LOG.error("Unknown error while trying to fetch index readers.",e);
			throw new BlurException(e.getMessage());
		}
		Filter preFilter = parseFilter(table, preSuperFilter, false, type);
		Filter postFilter = parseFilter(table, postSuperFilter, true, type);
		final Query userQuery = parseQuery(query,superQueryOn, indexServer.getAnalyzer(table), postFilter, preFilter, type);
		return ForkJoin.execute(executor, indexReaders.entrySet(), new ParallelCall<Entry<String, IndexReader>, HitsIterable>() {
			@Override
			public HitsIterable call(Entry<String, IndexReader> entry) throws Exception {
			    IndexReader reader = entry.getValue();
		        String shard = entry.getKey();
                BlurSearcher searcher = new BlurSearcher(reader, PrimeDocCache.getTableCache().getShardCache(table).getIndexReaderCache(shard));
		        searcher.setSimilarity(indexServer.getSimilarity());
			    return new HitsIterableSearcher((Query) userQuery.clone(), table, shard, searcher);
			}
		}).merge(new MergerHitsIterable(minimumNumberOfHits,maxQueryTime));
	}

	private Filter parseFilter(String table, String filter, boolean superQueryOn, ScoreType scoreType) throws ParseException, BlurException {
		if (filter == null) {
			return null;
		}
		return new QueryWrapperFilter(new SuperParser(LUCENE_VERSION, indexServer.getAnalyzer(table), superQueryOn, null, scoreType).parse(filter));
	}

	private Query parseQuery(String query, boolean superQueryOn, Analyzer analyzer, Filter postFilter, Filter preFilter, ScoreType scoreType) throws ParseException {
		Query result = new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, preFilter, scoreType).parse(query);
		if (postFilter == null) {
			return result;	
		}
		return new FilteredQuery(result, postFilter);
	}
	
	private Iterable<Document> getDocs(final IndexReader reader, String id) throws IOException {
		TermDocs termDocs = reader.termDocs(new Term(SuperDocument.ID,id));
		return new TermDocIterable(termDocs,reader);
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
	
	public static boolean sleep(long pauseTime) {
		try {
			Thread.sleep(pauseTime);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}
	
	private void checkIfShardIsNull(Object shard) throws MissingShardException {
		if (shard == null) {
			throw new MissingShardException();
		}
	}
	
    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }
}
