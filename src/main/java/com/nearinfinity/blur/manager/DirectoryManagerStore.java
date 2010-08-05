package com.nearinfinity.blur.manager;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public interface DirectoryManagerStore {

	URI getURIForShardId(String table, String shardId);

	Map<String, Set<String>> getShardIdsToServe();

}
