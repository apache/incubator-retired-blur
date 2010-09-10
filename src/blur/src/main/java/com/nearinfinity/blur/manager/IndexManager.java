package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.manager.util.TermDocIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SuperColumn;
import com.nearinfinity.blur.thrift.generated.SuperColumnFamily;
import com.nearinfinity.mele.Mele;

public class IndexManager {

	private static final Log LOG = LogFactory.getLog(IndexManager.class);
	private Mele mele;
	private Map<String, Map<String, IndexWriter>> indexWriters = new ConcurrentHashMap<String, Map<String, IndexWriter>>();
	private Map<String, Analyzer> analyzers = new ConcurrentHashMap<String, Analyzer>();
	private Map<String, Partitioner> partitioners = new ConcurrentHashMap<String, Partitioner>();

	public IndexManager() throws IOException {
		setupMele();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				close();
			}
		}));
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
	
	public static void replace(IndexWriter indexWriter, SuperDocument document) throws IOException {
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

	private void setupMele() throws IOException {
		mele = Mele.getMele();
		List<String> listClusters = mele.listClusters();
		for (String cluster : listClusters) {
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
		for (String cluster : listClusters) {
			Map<String, IndexWriter> map = indexWriters.get(cluster);
			if (map == null) {
				map = new ConcurrentHashMap<String, IndexWriter>();
				indexWriters.put(cluster, map);
			}
			List<String> listDirectories = mele.listDirectories(cluster);
			openForWriting(cluster, listDirectories, map);
		}
	}

	private void setupPartitioner(String cluster) throws IOException {
		partitioners.put(cluster, new Partitioner(mele.listDirectories(cluster)));
	}

	private void openForWriting(String cluster, List<String> dirs, Map<String, IndexWriter> writersMap) throws IOException {
		for (String local : dirs) {
			// @todo get zk lock here....??? maybe if the index writer cannot
			// get a lock that is good enough???
			// also maybe send this into a loop so that if a node goes down it
			// will picks the pieces...
			// need to think about what happens when a node comes back online.
			Directory directory = mele.open(cluster, local);
			IndexDeletionPolicy deletionPolicy = mele.getIndexDeletionPolicy(cluster, local);
			Analyzer analyzer = analyzers.get(cluster);
			if (analyzer == null) {
				analyzer = new StandardAnalyzer(Version.LUCENE_30);
			}
			try {
				if (!IndexWriter.isLocked(directory)) {
					IndexWriter indexWriter = new IndexWriter(directory, analyzer, deletionPolicy, MaxFieldLength.UNLIMITED);
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

	public static SuperDocument createSuperDocument(Row row) {
		SuperDocument document = new SuperDocument(row.id);
		for (Entry<String, SuperColumnFamily> superColumnFamilyEntry : row.superColumnFamilies.entrySet()) {
			addSuperColumnFamily(superColumnFamilyEntry,document);
		}
		return document;
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
