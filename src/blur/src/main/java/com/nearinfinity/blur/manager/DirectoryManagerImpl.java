package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nearinfinity.blur.lucene.store.URIDirectory;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;

public class DirectoryManagerImpl implements DirectoryManager, BlurConstants {
	
	private static final int DEFAULT_NUMBER_OF_SHARDS_TO_SERVE_PER_PASS = 1;
	private final static Logger LOG = LoggerFactory.getLogger(DirectoryManagerImpl.class);
	private BlurConfiguration configuration = new BlurConfiguration();
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
		LOG.info("New Directories {}",newDirectories);
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
		URI uri = dao.getDirectoryURIToServe(table, shardId);
		return URIDirectory.openDirectory(uri);
	}

	private Map<String,Set<String>> getShardIdsToServe() {
		return dao.getShardIdsToServe(configuration.getNodeUuid(),configuration.getInt(BLUR_SHARDS_TOSERVE_PER_PASS, DEFAULT_NUMBER_OF_SHARDS_TO_SERVE_PER_PASS));
	}

}
