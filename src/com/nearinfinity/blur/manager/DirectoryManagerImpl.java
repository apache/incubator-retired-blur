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
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;

public class DirectoryManagerImpl implements DirectoryManager {
	private static final Log LOG = LogFactory.getLog(DirectoryManagerImpl.class);
	private volatile Map<String, Directory> directories = new TreeMap<String, Directory>();
	private DirectoryManagerDao dao;

	public DirectoryManagerImpl(DirectoryManagerDao dao) {
		this.dao = dao;
	}

	@Override
	public Map<String, Directory> getCurrentDirectories() {
		return directories;
	}

	@Override
	public void update() {
		updateDirectories();
	}

	private void updateDirectories() {
		TreeMap<String, Directory> newDirectories = new TreeMap<String, Directory>();
		Set<String> shardsToServe = getShardNamesToServe();
		for (String shardToServe : shardsToServe) {
			System.out.println("Getting shard [" + shardToServe + "]");
			Directory directory = directories.get(shardToServe);
			if (directory == null) {
				try {
					directory = openDirectory(shardToServe);
				} catch (IOException e) {
					LOG.error("Error opening directory", e);
				}
			}
			newDirectories.put(shardToServe, directory);
		}
		System.out.println("New dirs [" + newDirectories + "]");
		directories = newDirectories;
	}

	private Directory openDirectory(String shardToServe) throws IOException {
		URI uri = dao.getURIForShardId(shardToServe);
		return URIDirectory.openDirectory(uri);
	}

	private Set<String> getShardNamesToServe() {
		return dao.getShardNamesToServe();
	}

}
