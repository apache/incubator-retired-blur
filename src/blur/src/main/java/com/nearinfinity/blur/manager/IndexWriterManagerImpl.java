package com.nearinfinity.blur.manager;

import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.data.DataStorage;

public class IndexWriterManagerImpl implements IndexWriterManager {
	
	private DirectoryManager directoryManager;
	private DataStorage storage;
	private Map<String, Map<String, Directory>> directoriesToKeepUpToDate;
	private Map<String, Map<String, IndexWriterTask>> indexWriters = new TreeMap<String, Map<String,IndexWriterTask>>();
	

	public IndexWriterManagerImpl(DirectoryManager directoryManager, DataStorage storage) {
		this.directoryManager = directoryManager;
		this.storage = storage;
	}

	@Override
	public void update() {
		updateIndexWriters(directoryManager.getCurrentDirectories());
	}

	private void updateIndexWriters(Map<String, Map<String, Directory>> currentDirectories) {
		this.directoriesToKeepUpToDate = currentDirectories;
		
		//this checks what to start
		for (String table : directoriesToKeepUpToDate.keySet()) {
			Map<String, Directory> shardsOfDirs = directoriesToKeepUpToDate.get(table);
			Map<String, IndexWriterTask> shardsOfWriters = indexWriters.get(table);
			if (shardsOfWriters == null) {
				shardsOfWriters = new TreeMap<String, IndexWriterTask>();
				indexWriters.put(table, shardsOfWriters);
			}
			for (String shardId : shardsOfDirs.keySet()) {
				if (!isServingIndexWriter(table,shardId)) {
					IndexWriterTask task = new IndexWriterTask(table,shardId,shardsOfDirs.get(shardId), storage);
					shardsOfWriters.put(shardId, task);
					task.start();
				}
			}
		}
		
		//this checks what to stop
		for (String table : indexWriters.keySet()) {
			Map<String, IndexWriterTask> shardsOfWriters = indexWriters.get(table);
			if (!directoriesToKeepUpToDate.containsKey(table)) {
				close(shardsOfWriters);
			} else {
				Map<String, Directory> shardsOfDirs = directoriesToKeepUpToDate.get(table);
				for (String shardId : shardsOfWriters.keySet()) {
					if (!shardsOfDirs.containsKey(shardId)) {
						close(shardsOfWriters.get(shardId));
					}
				}
			}
		}
	}

	private void close(IndexWriterTask indexWriter) {
		indexWriter.stop();
	}

	private void close(Map<String, IndexWriterTask> shardsOfWriters) {
		for (IndexWriterTask writer : shardsOfWriters.values()) {
			close(writer);
		}
	}

	private boolean isServingIndexWriter(String table, String shardId) {
		Map<String, IndexWriterTask> shardsOfWriters = indexWriters.get(table);
		if (shardsOfWriters == null) {
			return false;
		}
		return shardsOfWriters.containsKey(shardId);
	}

}
