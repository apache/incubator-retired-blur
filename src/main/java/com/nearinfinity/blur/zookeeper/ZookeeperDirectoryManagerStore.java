package com.nearinfinity.blur.zookeeper;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import com.nearinfinity.blur.manager.dao.DirectoryManagerStore;

public class ZookeeperDirectoryManagerStore implements DirectoryManagerStore {

	@Override
	public Map<String, Set<String>> getShardIdsToServe() {
		return null;
	}

	@Override
	public URI getURIForShardId(String table, String shardId) {
		return null;
	}

}
