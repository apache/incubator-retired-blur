package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;

import com.nearinfinity.mele.Mele;

public class IndexManager {

//	private static final Log LOG = LogFactory.getLog(IndexManagerImpl.class);
	private Mele mele;
	private Map<String,Map<String,IndexWriter>> indexWriters = new ConcurrentHashMap<String, Map<String,IndexWriter>>();
	private Map<String,Analyzer> analyzers = new ConcurrentHashMap<String, Analyzer>();

	public IndexManager() throws IOException {
		setupMele();
	}

	private void setupMele() throws IOException {
		mele = Mele.getMele();
		List<String> listClusters = mele.listClusters();
		for (String cluster : listClusters) {
			Map<String, IndexWriter> map = indexWriters.get(cluster);
			if (map == null) {
				map = new ConcurrentHashMap<String, IndexWriter>();
				indexWriters.put(cluster, map);
			}
			List<String> localDirectories = mele.listLocalDirectories(cluster);
			for (String local : localDirectories) {
				//@todo get zk lock here....???  maybe if the index writer cannot get a lock that is good enough???
				// also maybe send this into a loop so that if a node goes down it will picks the pieces...
				// need to think about what happens when a node comes back online.
				Directory directory = mele.open(cluster, local);
				IndexDeletionPolicy deletionPolicy = mele.getIndexDeletionPolicy(cluster, local);
				Analyzer analyzer = analyzers.get(cluster);
				IndexWriter indexWriter = new IndexWriter(directory, analyzer, deletionPolicy, MaxFieldLength.UNLIMITED);
				map.put(local, indexWriter);
			}
		}
		
		//@todo once all are brought online, look for shards that are not online yet...
	}

	public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
		Map<String, IndexWriter> map = indexWriters.get(table);
		Map<String, IndexReader> reader = new HashMap<String, IndexReader>();
		for (Entry<String, IndexWriter> writer : map.entrySet()) {
			reader.put(writer.getKey(), writer.getValue().getReader());
		}
		return reader;
	}
}
