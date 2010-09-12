package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.lucene.search.FilterParser;
import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.manager.util.TermDocIterable;
import com.nearinfinity.blur.thrift.BlurAdminServer.HitsMerger;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.thrift.generated.SuperColumnFamily;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.mele.Mele;

public class IndexManager {

	private static final Log LOG = LogFactory.getLog(IndexManager.class);
	private static final long POLL_TIME = 5000;
	private static final TableManager ALWAYS_ON = new TableManager() {
		@Override
		public boolean isTableEnabled(String table) {
			return true;
		}

		@Override
		public String getAnalyzerDefinitions(String table) {
			return "";
		}
	};
	
	private Mele mele;
	private Map<String, Map<String, IndexWriter>> indexWriters = new ConcurrentHashMap<String, Map<String, IndexWriter>>();
	private Map<String, Analyzer> analyzers = new ConcurrentHashMap<String, Analyzer>();
	private Map<String, Partitioner> partitioners = new ConcurrentHashMap<String, Partitioner>();
	private FilterManager filterManager;
	private Similarity similarity = new FairSimilarity();
	private ExecutorService executor = Executors.newCachedThreadPool();
	private TableManager manager;
	private Thread daemon;
	
	public interface TableManager {
		boolean isTableEnabled(String table);
		String getAnalyzerDefinitions(String table);
	}
	
	public IndexManager(TableManager manager) throws IOException, BlurException {
		this.manager = manager;
		setupIndexManager();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				close();
			}
		}));
	}

	public IndexManager() throws IOException, BlurException {
		this(ALWAYS_ON);
	}

	public Map<String, IndexReader> getIndexReaders(String table)
			throws IOException {
		Map<String, IndexWriter> map = indexWriters.get(table);
		Map<String, IndexReader> reader = new HashMap<String, IndexReader>();
		for (Entry<String, IndexWriter> writer : map.entrySet()) {
			reader.put(writer.getKey(), writer.getValue().getReader());
		}
		return reader;
	}

	public static void replace(IndexWriter indexWriter, Row row) throws IOException {
		replace(indexWriter,createSuperDocument(row));
	}
	
	public synchronized static void replace(IndexWriter indexWriter, SuperDocument document) throws IOException {
		indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
		if (!replaceInternal(indexWriter,document)) {
			indexWriter.deleteDocuments(new Term(SuperDocument.ID, document.getId()));
			if (!replaceInternal(indexWriter,document)) {
				throw new IOException("SuperDocument too large, try increasing ram buffer size.");
			}
		}
	}

	public void close() {
		for (Entry<String, Map<String, IndexWriter>> writers : indexWriters.entrySet()) {
			for (Entry<String, IndexWriter> writer : writers.getValue().entrySet()) {
				try {
					writer.getValue().close();
				} catch (IOException e) {
					LOG.error("Erroring trying to close [" + writers.getKey()
							+ "] [" + writer.getKey() + "]", e);
				}
			}
		}
	}

	public boolean appendRow(String table, Row row) {
		return false;
	}

	public boolean replaceRow(String table, Row row) throws BlurException {
		IndexWriter indexWriter = getIndexWriter(table, row.id);
		if (indexWriter != null) {
			try {
				replace(indexWriter,row);
			} catch (IOException e) {
				LOG.error("Unknown error",e);
				throw new BlurException("Unknown error [" + e.getMessage() + "]");
			}
			return true;
		}
		return false;
	}

	public boolean removeSuperColumn(String table, String id, String superColumnId) {
		return false;
	}

	public boolean removeRow(String table, String id) {
		return false;
	}

	public SuperColumn fetchSuperColumn(String table, String id, String superColumnFamilyName, String superColumnId) {
		return null;
	}

	public Row fetchRow(String table, String id) throws BlurException {
		try {
			IndexReader reader = getIndexReader(table,id);
			Iterable<Document> docs = getDocs(reader,id);
			Row row = new Row();
			row.id = id;
			boolean empty = true;
			for (Document document : docs) {
				empty = false;
				addDocumentToRow(row,document);
			}
			if (empty) {
				return null;
			}
			return row;
		} catch (IOException e) {
			LOG.error("Unknown io error",e);
			throw new BlurException(e.getMessage());
		}
	}
	
	public Hits search(String table, String query, boolean superQueryOn,
			ScoreType type, String filter, final long start, final int fetch,
			long minimumNumberOfHits, long maxQueryTime) throws BlurException {
		Map<String, IndexReader> indexReaders;
		try {
			indexReaders = getIndexReaders(table);
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

	private Analyzer getAnalyzer(String table) throws BlurException {
		Analyzer analyzer = analyzers.get(table);
		if (analyzer != null) {
			return analyzer;
		}
		try {
			BlurAnalyzer blurAnalyzer = BlurAnalyzer.create(manager.getAnalyzerDefinitions(table));
			analyzers.put(table, blurAnalyzer);
			return blurAnalyzer;
		} catch (Exception e) {
			LOG.error("unknown error", e);
			throw new BlurException(e.getMessage());
		}
	}

	private Query parse(String query, boolean superQueryOn, Analyzer analyzer, Filter queryFilter) throws ParseException {
		return new SuperParser(Version.LUCENE_30, analyzer, superQueryOn, queryFilter).parse(query);
	}
	
	private Hits performSearch(Query query, String shardId, IndexReader reader, int start, int fetch) throws IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		searcher.setSimilarity(similarity);
		int count = (int) (start + fetch);
		TopDocs topDocs = searcher.search(query, count == 0 ? 1 : count);
		return new Hits(topDocs.totalHits, getShardInfo(shardId,topDocs), fetch == 0 ? null : getHitList(searcher,(int) start,fetch,topDocs));
	}

	private Map<String, Long> getShardInfo(String shardId, TopDocs topDocs) {
		Map<String, Long> shardInfo = new TreeMap<String, Long>();
		shardInfo.put(shardId, (long)topDocs.totalHits);
		return shardInfo;
	}

	private List<Hit> getHitList(Searcher searcher, int start, int fetch, TopDocs topDocs) throws CorruptIndexException, IOException {
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

	public static SuperDocument createSuperDocument(Row row) {
		SuperDocument document = new SuperDocument(row.id);
		for (Entry<String, SuperColumnFamily> superColumnFamilyEntry : row.superColumnFamilies.entrySet()) {
			addSuperColumnFamily(superColumnFamilyEntry,document);
		}
		return document;
	}

	private void addDocumentToRow(Row row, Document document) {
		String superColumnId = document.getField(SuperDocument.SUPER_KEY).stringValue();
		SuperColumn superColumn = new SuperColumn();
		superColumn.id = superColumnId;
		superColumn.columns = new TreeMap<String, Column>();
		String superColumnFamily = null;
		for (Fieldable fieldable : document.getFields()) {
			String name = fieldable.name();
			int index = name.indexOf(SuperDocument.SEP);
			if (index < 0) {
				continue;
			}
			if (superColumnFamily == null) {
				superColumnFamily = name.substring(0,index);	
			}
			String columnName = name.substring(index + 1);
			String value = fieldable.stringValue();
			addValue(superColumn,columnName,value);
		}
		addToRow(row,superColumnFamily,superColumn);
	}

	private void addValue(SuperColumn superColumn, String columnName, String value) {
		Column column = superColumn.columns.get(columnName);
		if (column == null) {
			column = new Column();
			column.name = columnName;
			column.values = new ArrayList<String>();
			superColumn.columns.put(column.name, column);
		}
		column.values.add(value);
	}

	private void addToRow(Row row, String superColumnFamilyName, SuperColumn superColumn) {
		if (row.superColumnFamilies == null) {
			row.superColumnFamilies = new TreeMap<String, SuperColumnFamily>();
		}
		SuperColumnFamily superColumnFamily = row.superColumnFamilies.get(superColumnFamilyName);
		if (superColumnFamily == null) {
			superColumnFamily = new SuperColumnFamily();
			superColumnFamily.name = superColumnFamilyName;
			superColumnFamily.superColumns = new TreeMap<String, SuperColumn>();
			row.superColumnFamilies.put(superColumnFamily.name, superColumnFamily);
		}
		superColumnFamily.superColumns.put(superColumn.id, superColumn);
	}

	private Iterable<Document> getDocs(final IndexReader reader, String id) throws IOException {
		TermDocs termDocs = reader.termDocs(new Term(SuperDocument.ID,id));
		return new TermDocIterable(termDocs,reader);
	}

	private void setupIndexManager() throws IOException, BlurException {
		mele = Mele.getMele();
		List<String> listClusters = mele.listClusters();
		for (String cluster : listClusters) {
			if (!manager.isTableEnabled(cluster)) {
				ensureClosed(cluster);
				continue;
			}
			setupPartitioner(cluster);
			Map<String, IndexWriter> map = indexWriters.get(cluster);
			if (map == null) {
				map = new ConcurrentHashMap<String, IndexWriter>();
				indexWriters.put(cluster, map);
			}
			List<String> localDirectories = mele.listLocalDirectories(cluster);
			openForWriting(cluster, localDirectories, map);
		}

		// @todo once all are brought online, look for shards that are not
		// online yet...
		serverUnservedShards();
		daemon = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(POLL_TIME);
				} catch (InterruptedException e) {
					return;
				}
				while (true) {
					serverUnservedShards();
					try {
						Thread.sleep(POLL_TIME);
					} catch (InterruptedException e) {
						return;
					}
				}
			}
		});
		daemon.setDaemon(true);
		daemon.setName("IndexManager-Index-Daemon");
		daemon.start();
	}
	
	private void serverUnservedShards() {
		try {
			for (String cluster : mele.listClusters()) {
				if (!manager.isTableEnabled(cluster)) {
					ensureClosed(cluster);
					continue;
				}
				Map<String, IndexWriter> map = indexWriters.get(cluster);
				if (map == null) {
					map = new ConcurrentHashMap<String, IndexWriter>();
					indexWriters.put(cluster, map);
				}
				List<String> listDirectories = mele.listDirectories(cluster);
				openForWriting(cluster, listDirectories, map);
			}
		} catch (Exception e) {
			LOG.error("Unknown error",e);
		}
	}

	private void ensureClosed(String table) {
		Map<String, IndexWriter> writers = indexWriters.get(table);
		ensureClosed(writers);
	}

	private void ensureClosed(Map<String, IndexWriter> writers) {
		for (IndexWriter writer : writers.values()) {
			ensureClosed(writer);
		}
	}

	private void ensureClosed(IndexWriter writer) {
		try {
			writer.close();
		} catch (IOException e) {
			LOG.error("Uknown lucene error",e);
		}
	}

	private void setupPartitioner(String cluster) throws IOException {
		partitioners.put(cluster, new Partitioner(mele.listDirectories(cluster)));
	}

	private void openForWriting(String cluster, List<String> dirs, Map<String, IndexWriter> writersMap) throws IOException, BlurException {
		for (String local : dirs) {
			// @todo get zk lock here....??? maybe if the index writer cannot
			// get a lock that is good enough???
			// also maybe send this into a loop so that if a node goes down it
			// will picks the pieces...
			// need to think about what happens when a node comes back online.
			Directory directory = mele.open(cluster, local);
			IndexDeletionPolicy deletionPolicy = mele.getIndexDeletionPolicy(cluster, local);
			Analyzer analyzer = getAnalyzer(cluster);
			try {
				if (!IndexWriter.isLocked(directory)) {
					IndexWriter indexWriter = new IndexWriter(directory, analyzer, deletionPolicy, MaxFieldLength.UNLIMITED);
					indexWriter.setSimilarity(similarity);
					writersMap.put(local, indexWriter);
				}
			} catch (LockObtainFailedException e) {
				LOG.info("Cluster [" + cluster + "] shard [" + local + "] is locked by another shard.");
			}
		}
	}
	
	private IndexWriter getIndexWriter(String table, String id) throws BlurException {
		Partitioner partitioner = partitioners.get(table);
		if (id == null) {
			throw new BlurException("null mutation id");
		}
		String shard = partitioner.getShard(id);
		Map<String, IndexWriter> tableWriters = indexWriters.get(table);
		if (tableWriters == null) {
			LOG.error("Table [" + table + "] not online in this server.");
			throw new BlurException("Table [" + table
					+ "] not online in this server.");
		}
		IndexWriter indexWriter = tableWriters.get(shard);
		if (indexWriter == null) {
			LOG.error("Shard [" + shard + "] from table [" + table
					+ "] not online in this server.");
			return null;
		}
		return indexWriter;
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

	private static void addSuperColumnFamily(Entry<String, SuperColumnFamily> superColumnFamilyEntry, SuperDocument document) {
		String superColumnFamilyName = superColumnFamilyEntry.getKey();
		SuperColumnFamily superColumnFamily = superColumnFamilyEntry.getValue();
		for (Entry<String, SuperColumn> superColumnEntry : superColumnFamily.superColumns.entrySet()) {
			add(superColumnFamilyName, superColumnEntry, document);
		}
	}

	private static void add(String superColumnFamilyName, Entry<String, SuperColumn> superColumnEntry, SuperDocument document) {
		String superColumnId = superColumnEntry.getKey();
		SuperColumn superColumn = superColumnEntry.getValue();
		for (Entry<String, Column> columnEntry : superColumn.columns.entrySet()) {
			add(superColumnFamilyName, superColumnId, columnEntry,document);
		}		
	}

	private static void add(String superColumnFamilyName, String superColumnId, Entry<String, Column> columnEntry, SuperDocument document) {
		String columnName = columnEntry.getKey();
		Column column = columnEntry.getValue();
		for (String value : column.values) {
			document.addFieldStoreAnalyzedNoNorms(superColumnFamilyName, superColumnId, columnName, value);
		}
	}
	
	private IndexReader getIndexReader(String table, String id) throws BlurException {
		IndexWriter indexWriter = getIndexWriter(table, id);
		if (indexWriter == null) {
			return null;
		}
		try {
			return indexWriter.getReader();
		} catch (IOException e) {
			LOG.error("Error trying to open reader on writer.",e);
			throw new BlurException("Error trying to open reader on writer.");
		}
	}

}
