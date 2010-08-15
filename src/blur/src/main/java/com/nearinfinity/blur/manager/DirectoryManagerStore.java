package com.nearinfinity.blur.manager;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public interface DirectoryManagerStore {

	

	Map<String, Set<String>> getShardIdsToServe(String nodeId, int maxToServePerCall);
	
	void addDirectoryURIToServe(String table, String shardId, URI dirUri);
	void removeDirectoryURIToServe(String table, String shardId);
	URI getDirectoryURIToServe(String table, String shardId);
	Set<String> getTables();
	Set<String> getShardIds(String table);
	void removeTable(String table);
	boolean isThisNodeServing(String table, String shardId);
	boolean obtainLock(String table, String shardId);
	void releaseLock(String table, String shardId);

}
