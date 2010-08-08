package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class IndexReaderManagerImpl implements IndexReaderManager {

	private static final Log LOG = LogFactory.getLog(IndexReaderManagerImpl.class);
	private DirectoryManager directoryManager;
	private volatile Map<String,Map<String, SuperIndexReader>> readers = new TreeMap<String, Map<String,SuperIndexReader>>();

	public IndexReaderManagerImpl(DirectoryManager directoryManager) {
		this.directoryManager = directoryManager;
	}
	
	@Override
	public void update() {
		updateIndexReaders(directoryManager.getCurrentDirectories());
	}

	@Override
	public Map<String,Map<String, SuperIndexReader>> getCurrentIndexReaders() {
		return readers;
	}
	
	protected void updateIndexReaders(Map<String,Map<String, Directory>> directories) {
		Map<String,Map<String, SuperIndexReader>> newTableReaders = new TreeMap<String, Map<String,SuperIndexReader>>();
		for (String table : directories.keySet()) {
			Map<String, Directory> newDirectories = directories.get(table);
			Map<String,SuperIndexReader> newReaders = new TreeMap<String, SuperIndexReader>();
			for (Entry<String, Directory> entry : newDirectories.entrySet()) {
				try {
					newReaders.put(entry.getKey(), openReader(table, entry.getKey(), entry.getValue()));
				} catch (Exception e) {
					LOG.error("Error open new index for reading using shard [" + entry.getKey() + "]",e);
				}
			}
			newTableReaders.put(table, newReaders);
		}
		LOG.info("New Indexreaders [" + newTableReaders + "]");
		readers = newTableReaders;
	}

	private SuperIndexReader openReader(String table, String shardId, Directory dir) throws IOException, InterruptedException {
		SuperIndexReader reader = getReader(table,shardId);
		if (reader == null) {
			return new SuperIndexReader(dir).waitForWarmUp();
		} else if (!reader.isCurrent()) {
			return reader.reopenSuper().waitForWarmUp();
		}
		return reader;
	}

	private SuperIndexReader getReader(String table, String shardId) {
		Map<String, SuperIndexReader> tableMap = readers.get(table);
		if (tableMap != null) {
			return tableMap.get(shardId);
		}
		return null;
	}


}
