package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.lucene.store.URIDirectory;
import com.nearinfinity.blur.manager.dao.DirectoryManagerStore;

public class DirectoryManagerImpl implements DirectoryManager {
	private static final Log LOG = LogFactory.getLog(DirectoryManagerImpl.class);
	private volatile Map<String,Map<String, Directory>> directories = new TreeMap<String, Map<String, Directory>>();
	private DirectoryManagerStore dao;

	public DirectoryManagerImpl(DirectoryManagerStore dao) {
		this.dao = dao;
	}

	@Override
	public Map<String,Map<String, Directory>> getCurrentDirectories() {
		return directories;
	}

	@Override
	public void update() {
		updateDirectories();
	}

	private void updateDirectories() {
		Map<String,Map<String, Directory>> newDirectories = new TreeMap<String, Map<String, Directory>>();
		Map<String, Set<String>> shardNamesToServe = getShardIdsToServe();
		for (String table : shardNamesToServe.keySet()) {
			Map<String, Directory> newDirs = new TreeMap<String, Directory>();
			for (String shardId : shardNamesToServe.get(table)) {
				Directory directory = getCurrentDirectory(table,shardId);
				if (directory == null) {
					try {
						directory = openDirectory(table, shardId);
					} catch (IOException e) {
						LOG.error("Error opening directory", e);
					}
				}
				newDirs.put(shardId, directory);
			}
			newDirectories.put(table, newDirs);
		}
		LOG.info("New Directories [" + newDirectories + "]");
		directories = newDirectories;
	}

	private Directory getCurrentDirectory(String table, String shardId) {
		Map<String, Directory> map = directories.get(table);
		if (map != null) {
			return map.get(shardId);
		}
		return null;
	}

	private Directory openDirectory(String table, String shardId) throws IOException {
		URI uri = dao.getURIForShardId(table, shardId);
		return URIDirectory.openDirectory(uri);
	}

	private Map<String,Set<String>> getShardIdsToServe() {
		return dao.getShardIdsToServe();
	}

}
