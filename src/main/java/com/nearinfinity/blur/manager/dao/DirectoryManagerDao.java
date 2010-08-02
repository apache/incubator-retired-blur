package com.nearinfinity.blur.manager.dao;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public interface DirectoryManagerDao {

	URI getURIForShardId(String table, String shardId);

	Map<String, Set<String>> getShardIdsToServe();

}
