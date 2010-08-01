package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class IndexManagerImpl implements IndexManager {

	private static final Log LOG = LogFactoryImpl.getLog(IndexManagerImpl.class);
	private static final long WAIT_BETWEEN_PASSES = 10000;
	private DirectoryManager directoryManager;
	private Map<String, SuperIndexReader> readers = new TreeMap<String, SuperIndexReader>();
	private Timer timer;

	public IndexManagerImpl(DirectoryManager directoryManager) {
		this.directoryManager = directoryManager;
		this.timer = new Timer("IndexReader-Refresher",true);
		this.timer.scheduleAtFixedRate(getTask(), WAIT_BETWEEN_PASSES, WAIT_BETWEEN_PASSES);
	}

	@Override
	public Map<String, SuperIndexReader> getCurrentIndexReaders() {
		return readers;
	}
	
	private TimerTask getTask() {
		return new TimerTask() {
			@Override
			public void run() {
				updateIndexers(directoryManager.getCurrentDirectories());
			}
		};
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
