package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.search.FilterParser;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.manager.FilterManager;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	private Map<String,Analyzer> analyzerCache = new ConcurrentHashMap<String, Analyzer>();
	private FilterManager filterManager;
	private Similarity similarity;
	
	public BlurShardServer() throws IOException {
		super();
		startServer();
	}
	
	private void startServer() throws IOException {
		this.indexManager = new IndexManager();
		this.similarity = configuration.getNewInstance(BLUR_LUCENE_SIMILARITY_CLASS, Similarity.class);
	}

	@Override
	public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String filter, 
			final long start, final int fetch, long minimumNumberOfHits, long maxQueryTime) throws BlurException, TException {
		Map<String, IndexReader> indexReaders;
		try {
			indexReaders = indexManager.getIndexReaders(table);
		} catch (IOException e) {
			LOG.error("Unknown error",e);
			throw new BlurException(e.getMessage());
		}
		try {
			Filter queryFilter = parse(table,filter);
			final Query userQuery = parse(query,superQueryOn,getAnalyzer(table),queryFilter);
			return ForkJoin.execute(executor, indexReaders.entrySet(), new ParallelCall<Entry<String, IndexReader>, Hits>() {
				@Override
				public Hits call(Entry<String, IndexReader> entry) throws Exception {
					return performSearch((Query) userQuery.clone(), entry.getKey(), entry.getValue(), (int) start, fetch);
				}
			}).merge(new HitsMerger());
		} catch (Exception e) {
			LOG.error("Unknown error",e);
			throw new BlurException(e.getMessage());
		}
	}

	private Filter parse(String table, String filter) throws ParseException {
		return new FilterParser(table, filterManager).parseFilter(filter);
	}

	private Analyzer getAnalyzer(String table) throws BlurException, TException {
		Analyzer analyzer = analyzerCache.get(table);
		if (analyzer != null) {
			return analyzer;
		}
		TableDescriptor descriptor = describe(table);
		if (descriptor == null) {
			throw new BlurException("Descriptor for table [" + table +
					"] cannot be null");
		}
		try {
			BlurAnalyzer blurAnalyzer = BlurAnalyzer.create(descriptor.analyzerDef);
			analyzerCache.put(table, blurAnalyzer);
			return blurAnalyzer;
		} catch (Exception e) {
			LOG.error("unknown error", e);
			throw new BlurException(e.getMessage());
		}
	}

	protected Hits performSearch(Query query, String shardId, IndexReader reader, int start, int fetch) throws IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(similarity);
		int count = (int) (start + fetch);
		TopDocs topDocs = searcher.search(query, count == 0 ? 1 : count);
		return new Hits(topDocs.totalHits, getShardInfo(shardId,topDocs), fetch == 0 ? null : getHitList(searcher,(int) start,fetch,topDocs));
	}

	protected Map<String, Long> getShardInfo(String shardId, TopDocs topDocs) {
		Map<String, Long> shardInfo = new TreeMap<String, Long>();
		shardInfo.put(shardId, (long)topDocs.totalHits);
		return shardInfo;
	}

	protected List<Hit> getHitList(Searcher searcher, int start, int fetch, TopDocs topDocs) throws CorruptIndexException, IOException {
		List<Hit> hitList = new ArrayList<Hit>();
		int collectTotal = start + fetch;
		ScoreDoc[] scoreDocs = topDocs.scoreDocs;
		for (int i = start; i < topDocs.scoreDocs.length && i < collectTotal; i++) {
			ScoreDoc scoreDoc = scoreDocs[i];
			String id = fetchId(searcher, scoreDoc.doc);
			hitList.add(new Hit(id, scoreDoc.score, null));
		}
		return hitList;
	}

	private String fetchId(Searcher searcher, int docId) throws CorruptIndexException, IOException {
		Document doc = searcher.doc(docId);
		return doc.get(SuperDocument.ID);
	}

	private Query parse(String query, boolean superQueryOn, Analyzer analyzer, Filter queryFilter) throws ParseException {
		return new SuperParser(Version.LUCENE_30, analyzer, superQueryOn, queryFilter).parse(query);
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.NODE;
	}

	@Override
	public Row fetchRow(String table, String id) throws BlurException, TException {
		return indexManager.fetchRow(table,id);
	}

	@Override
	public SuperColumn fetchSuperColumn(String table, String id, String superColumnFamilyName, String superColumnId) throws BlurException, TException {
		return indexManager.fetchSuperColumn(table,id,superColumnFamilyName,superColumnId);
	}
	
	@Override
	public boolean appendRow(String table, Row row) throws BlurException, TException {
		return indexManager.appendRow(table,row);
	}

	@Override
	public boolean removeRow(String table, String id) throws BlurException, TException {
		return indexManager.removeRow(table,id);
	}

	@Override
	public boolean removeSuperColumn(String table, String id, String superColumnId) throws BlurException, TException {
		return indexManager.removeSuperColumn(table,id,superColumnId);
	}

	@Override
	public boolean replaceRow(String table, Row row) throws BlurException, TException {
		return indexManager.replaceRow(table,row);
	}


}
