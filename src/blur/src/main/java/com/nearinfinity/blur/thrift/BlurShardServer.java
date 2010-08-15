package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.DirectoryManagerStore;
import com.nearinfinity.blur.manager.IndexReaderManagerImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Logger LOG = LoggerFactory.getLogger(BlurShardServer.class);
	private static final long TIME_BETWEEN_UPDATES = 10000;
	private DirectoryManagerImpl directoryManager;
	private IndexReaderManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private Timer timer;
	private Map<String,Analyzer> analyzerCache = new ConcurrentHashMap<String, Analyzer>();
	
	public BlurShardServer() throws IOException {
		super();
		startServer();
	}
	
	private void startServer() throws IOException {
		DirectoryManagerStore dao = configuration.getNewInstance(BLUR_DIRECTORY_MANAGER_STORE_CLASS, DirectoryManagerStore.class);
		this.directoryManager = new DirectoryManagerImpl(dao);
		this.indexManager = new IndexReaderManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		update(directoryManager, indexManager, searchManager);
		runUpdateTask(directoryManager, indexManager, searchManager);
	}

	@Override
	public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String filter, 
			final long start, final int fetch, long minimumNumberOfHits, long maxQueryTime) throws BlurException, TException {
		Map<String, Searcher> searchers = searchManager.getSearchers(table);
		try {
			final Query q = parse(query,superQueryOn,getAnalyzer(table));
			return ForkJoin.execute(executor, searchers.entrySet(), new ParallelCall<Entry<String, Searcher>, Hits>() {
				@Override
				public Hits call(Entry<String, Searcher> entry) throws Exception {
					return performSearch((Query) q.clone(), entry.getKey(), entry.getValue(), (int) start, fetch);
				}
			}).merge(new HitsMerger());
		} catch (Exception e) {
			LOG.error("Unknown error",e);
			throw new BlurException(e.getMessage());
		}
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

	protected Hits performSearch(Query query, String shardId,Searcher searcher, int start, int fetch) throws IOException {
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

	private Query parse(String query, boolean superQueryOn, Analyzer analyzer) throws ParseException {
		return new SuperParser(Version.LUCENE_CURRENT, analyzer, superQueryOn).parse(query);
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.NODE;
	}
	
	private void runUpdateTask(final UpdatableManager... managers) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				update(managers);
			}
		};
		this.timer = new Timer("Update-Manager-Timer", true);
		this.timer.schedule(task, TIME_BETWEEN_UPDATES, TIME_BETWEEN_UPDATES);
	}
	
	private void update(UpdatableManager... managers) {
		LOG.info("Running Update");
		for (UpdatableManager manager : managers) {
			manager.update();
		}
	}
}
