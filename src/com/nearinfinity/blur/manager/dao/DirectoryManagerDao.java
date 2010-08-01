package com.nearinfinity.blur.manager.dao;

import java.net.URI;
import java.util.Set;

public interface DirectoryManagerDao {

	URI getURIForShardId(String shardId);

	Set<String> getShardNamesToServe();

}
