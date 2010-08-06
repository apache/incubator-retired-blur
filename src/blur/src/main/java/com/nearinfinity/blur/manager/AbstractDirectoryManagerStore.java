package com.nearinfinity.blur.manager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public abstract class AbstractDirectoryManagerStore implements DirectoryManagerStore {

	private Map<String, Set<String>> currentlyServing = new HashMap<String, Set<String>>();
	
	@Override
	public synchronized Map<String, Set<String>> getShardIdsToServe(String nodeId, int maxToServePerCall) {
		int count = 0;
		Set<String> tables = getTables();
		if (tables == null) {
			return new HashMap<String, Set<String>>();
		}
		for (String table : tables) {
			Set<String> shards = currentlyServing.get(table);
			if (shards == null) {
				shards = new TreeSet<String>();
				currentlyServing.put(table, shards);
			}
			Set<String> shardIds = getShardIds(table);
			for (String shardId : shardIds) {
				if (!isThisNodeServing(table,shardId)) {
					if (obtainLock(table,shardId)) {
						shards.add(shardId);
						count++;
						if (count >= maxToServePerCall) {
							return currentlyServing;
						}
					}
				}
			}
		}
		return currentlyServing;
	}
	
	@Override
	public synchronized boolean isThisNodeServing(String table, String shardId) {
		Set<String> shards = currentlyServing.get(table);
		if (shards == null) {
			return false;
		}
		return shards.contains(shardId);
	}
	
}
