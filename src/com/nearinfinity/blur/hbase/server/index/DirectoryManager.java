package com.nearinfinity.blur.hbase.server.index;

import java.util.Map;

import org.apache.lucene.store.Directory;

public interface DirectoryManager {
	
	Map<String,Directory> getCurrentDirectories();

}
