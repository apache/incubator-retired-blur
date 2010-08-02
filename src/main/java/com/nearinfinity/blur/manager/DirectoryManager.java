package com.nearinfinity.blur.manager;

import java.util.Map;

import org.apache.lucene.store.Directory;

public interface DirectoryManager extends UpdatableManager {
	
	Map<String,Directory> getCurrentDirectories();

}
