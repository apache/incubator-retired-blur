package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class IndexManagerImpl implements IndexManager {

	private static final Log LOG = LogFactoryImpl.getLog(IndexManagerImpl.class);
	private DirectoryManager directoryManager;
	private volatile Map<String, SuperIndexReader> readers = new TreeMap<String, SuperIndexReader>();

	public IndexManagerImpl(DirectoryManager directoryManager) {
		this.directoryManager = directoryManager;
	}
	
	@Override
	public void update() {
		updateIndexers(directoryManager.getCurrentDirectories());
	}

	@Override
	public Map<String, SuperIndexReader> getCurrentIndexReaders() {
		return readers;
	}
	
	protected void updateIndexers(Map<String, Directory> directories) {
		Map<String,SuperIndexReader> newReaders = new TreeMap<String, SuperIndexReader>();
		for (Entry<String, Directory> entry : directories.entrySet()) {
			try {
				newReaders.put(entry.getKey(), openReader(entry.getKey(), entry.getValue()));
			} catch (Exception e) {
				LOG.error("Error open new index for reading using shard [" + entry.getKey() + "]",e);
			}
		}
		System.out.println("new Indexes [" + readers + "]");
		readers = newReaders;
	}

	private SuperIndexReader openReader(String shardId, Directory dir) throws IOException, InterruptedException {
		IndexReader reader = readers.get(shardId);
		if (reader == null) {
			return new SuperIndexReader(dir).waitForWarmUp();
		}
		SuperIndexReader indexReader = readers.get(shardId);
		if (!indexReader.isCurrent()) {
			return indexReader.reopenSuper().waitForWarmUp();
		}
		return indexReader;
	}


}
