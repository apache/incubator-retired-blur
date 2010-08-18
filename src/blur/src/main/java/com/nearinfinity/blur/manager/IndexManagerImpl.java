package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.lucene.index.SuperIndexReader;

public class IndexManagerImpl implements IndexManager {

	private final static Logger LOG = LoggerFactory.getLogger(IndexManagerImpl.class);
	protected static final long WAIT_TIME_BEFORE_FORCING_CLOSED = 60000;
	private DirectoryManager directoryManager;
	private volatile Map<String,Map<String, SuperIndexReader>> readers = new TreeMap<String, Map<String,SuperIndexReader>>();
	private volatile Map<SuperIndexReader,Directory> dirs = new HashMap<SuperIndexReader, Directory>();

	public IndexManagerImpl(DirectoryManager directoryManager) {
		this.directoryManager = directoryManager;
	}

	@Override
	public Directory getDirectory(SuperIndexReader indexReader) {
		return dirs.get(indexReader);
	}
	
	@Override
	public void update() {
		updateIndexReaders(directoryManager.getCurrentDirectories());
	}

	protected void updateIndexReaders(Map<String,Map<String, Directory>> directories) {
		Map<String,Map<String, SuperIndexReader>> newTableReaders = new TreeMap<String, Map<String,SuperIndexReader>>();
		Map<SuperIndexReader, Directory> newDirs = new HashMap<SuperIndexReader, Directory>();
		for (String table : directories.keySet()) {
			Map<String, Directory> newDirectories = directories.get(table);
			Map<String,SuperIndexReader> newReaders = new TreeMap<String, SuperIndexReader>();
			for (Entry<String, Directory> entry : newDirectories.entrySet()) {
				try {
					Directory directory = entry.getValue();
					SuperIndexReader reader = openReader(table, entry.getKey(), directory);
					newDirs.put(reader, directory);
					newReaders.put(entry.getKey(), reader);
				} catch (Exception e) {
					LOG.error("Error open new index for reading using shard {}",entry.getKey());
					LOG.error("Unknown", e);
				}
			}
			newTableReaders.put(table, newReaders);
		}
		LOG.info("New Indexreaders {}",newTableReaders);
		Map<String,Map<String, SuperIndexReader>> oldTableReaders = readers;
		readers = newTableReaders;
		
		dirs = newDirs;
		//close old readers here?
		Collection<SuperIndexReader> readersThatNeedToBeClosed = getReadersThatNeedToBeClosed(oldTableReaders);
		futureClose(readersThatNeedToBeClosed);
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

	private void futureClose(final Collection<SuperIndexReader> readersThatNeedToBeClosed) {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(WAIT_TIME_BEFORE_FORCING_CLOSED);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				for (SuperIndexReader reader : readersThatNeedToBeClosed) {
					try {
						LOG.info("Closing reader {}",reader);
						reader.close();
					} catch (IOException e) {
						LOG.error("Unknown error",e);
					}
				}
			}
		});
		thread.setDaemon(true);
		thread.setName("Closing-Old-Readers-" + System.currentTimeMillis());
		thread.start();
	}

	private Collection<SuperIndexReader> getReadersThatNeedToBeClosed(Map<String, Map<String, SuperIndexReader>> oldTableReaders) {
		Collection<SuperIndexReader> result = new HashSet<SuperIndexReader>();
		for (String table : oldTableReaders.keySet()) {
			if (readers.containsKey(table)) {
				Map<String, SuperIndexReader> onlineReaders = readers.get(table);
				Map<String, SuperIndexReader> oldReaders = oldTableReaders.get(table);
				for (String shardId : oldReaders.keySet()) {
					if (!onlineReaders.containsKey(shardId)) {
						result.add(oldReaders.get(shardId));
					}
				}
			} else {
				//the whole table is gone
				Map<String, SuperIndexReader> map = oldTableReaders.get(table);
				result.addAll(map.values());
			}
		}
		LOG.info("Old readers that need to be closed {}",result);
		return result;
	}

	@Override
	public Map<String, SuperIndexReader> getIndexReaders(String table) {
		return readers.get(table);
	}
}
